import json
import boto3
import uuid
import os
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from datetime import timedelta
import json
from shared.database import DynamoDB
from shared.events import EventBridge

# Inicialización lazy: No crear globales en import time
dynamodb = None
events = None
sns = None  # NUEVO: Para SNS

def _get_dynamodb():
    global dynamodb
    if dynamodb is None:
        dynamodb = DynamoDB()
    return dynamodb

def _get_events():
    global events
    if events is None:
        events = EventBridge()
    return events

def _get_sns():  # NUEVO: Lazy init para SNS
    global sns
    if sns is None:
        sns = boto3.client('sns')
    return sns

# === FUNCIONES DEL CLIENTE (API para crear/obtener customers y orders) ===

# ==================== CREATE_ORDER CON VALIDACIÓN DE CLIENTE ====================
def create_order(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        
        # 1. Validar que el cliente exista
        customer_id = body['customerId']
        customer_pk = f"TENANT#pardos#CUSTOMER#{customer_id}"
        customer_resp = _get_dynamodb().get_item(
            table_name=os.environ['CUSTOMERS_TABLE'],
            key={'PK': customer_pk}
        )
        if 'Item' not in customer_resp:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Cliente no encontrado', 'customerId': customer_id})
            }

        tenant_id = body.get('tenantId', 'pardos')
        order_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()

        # Convertir precios a Decimal
        items_input = body.get('items', [])
        items_with_decimal = []
        total = Decimal('0')
        for item in items_input:
            price = Decimal(str(item['price']))
            qty = int(item['qty'])
            items_with_decimal.append({
                'productId': item['productId'],
                'qty': qty,
                'price': price
            })
            total += price * qty

        # Guardar pedido en PENDING_VALIDATION
        order_metadata = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': 'INFO',
            'orderId': order_id,
            'customerId': customer_id,
            'tenantId': tenant_id,
            'status': 'PENDING_VALIDATION',
            'items': items_with_decimal,
            'total': total,
            'createdAt': timestamp,
            'currentStep': 'VALIDATING_STOCK'
        }
        _get_dynamodb().put_item(os.environ['ORDERS_TABLE'], order_metadata)

        # Publicar evento
        items_for_event = [
            {"productId": i["productId"], "qty": i["qty"], "price": float(i["price"])}
            for i in items_with_decimal
        ]
        _get_events().publish_event(
            source="pardos.orders",
            detail_type="OrderCreated",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'total': float(total),
                'items': items_for_event,
                'timestamp': timestamp
            }
        )

        return {
            'statusCode': 201,
            'body': json.dumps({
                'orderId': order_id,
                'message': 'Pedido recibido, validando inventario...',
                'status': 'PENDING_VALIDATION'
            })
        }

    except Exception as e:
        print(f"Error en create_order: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def get_orders_by_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        tenant_id = 'pardos'
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='customerId = :cid',
            expression_attribute_values={':cid': customer_id}
        )
        orders = [item for item in response.get('Items', []) if item.get('PK', '').startswith(f"TENANT#{tenant_id}")]
        return {
            'statusCode': 200,
            'body': json.dumps({'orders': orders}, default=str)  # default=str para Decimal
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def create_customer(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        customer_id = str(uuid.uuid4())
        tenant_id = body.get('tenantId', 'pardos')
        timestamp = datetime.utcnow().isoformat()
        customer = {
            'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}",
            'customerId': customer_id,
            'tenantId': tenant_id,
            'name': body.get('name'),
            'email': body.get('email'),
            'createdAt': timestamp
        }
        _get_dynamodb().put_item(os.environ['CUSTOMERS_TABLE'], customer)
        return {
            'statusCode': 201,
            'body': json.dumps({'customerId': customer_id, 'message': 'Customer created'})
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def get_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        tenant_id = 'pardos'
        response = _get_dynamodb().get_item(
            table_name=os.environ['CUSTOMERS_TABLE'],
            key={'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}"}
        )
        item = response.get('Item')
        if not item:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Customer not found'})}
        return {
            'statusCode': 200,
            'body': json.dumps(item, default=str)
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def get_order(event, context):
    try:
        order_id = event['pathParameters']['orderId']
        tenant_id = 'pardos'
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"

        order_response = _get_dynamodb().query(
            table_name=os.environ['ORDERS_TABLE'],
            key_condition_expression='PK = :pk AND SK = :sk',
            expression_attribute_values={
                ':pk': pk,
                ':sk': 'INFO'
            }
        )
        order = order_response.get('Items', [{}])[0]
        if not order:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Order not found'})}

        # Join con customer
        customer_pk = f"TENANT#{tenant_id}#CUSTOMER#{order['customerId']}"
        customer_response = _get_dynamodb().get_item(
            table_name=os.environ['CUSTOMERS_TABLE'],
            key={'PK': customer_pk}
        )
        customer = customer_response.get('Item', {})

        # Join con steps
        steps_response = _get_dynamodb().query(
            table_name=os.environ['STEPS_TABLE'],
            key_condition_expression='PK = :pk',
            expression_attribute_values={':pk': pk}
        )
        steps = steps_response.get('Items', [])

        result = {
            'orderId': order_id,
            'status': order.get('status', 'PENDING_VALIDATION'),
            'currentStep': order.get('currentStep', 'VALIDATING_STOCK'),
            'total': float(order.get('total', 0)),
            'items': order.get('items', []),
            'customer': {
                'name': customer.get('name', 'N/A'),
                'email': customer.get('email', 'N/A')
            },
            'steps': [s['stepName'] for s in steps if 'stepName' in s],
            'createdAt': order.get('createdAt'),
            'rejectionReason': order.get('rejectionReason')  # NUEVO: muestra por qué fue rechazado
        }

        return {
            'statusCode': 200,
            'body': json.dumps(result, default=str)
        }

    except Exception as e:
        print(f"Error en get_order: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
    
# ==================== VALIDACIÓN DE INVENTARIO (Step Functions) ====================

def check_inventory(event, context):
    print("=== INICIANDO check_inventory ===")
    print("Event completo:", json.dumps(event, default=str))  # ← AGREGADO: default=str para Decimal/timestamps

    try:
        detail = event.get('detail', event)
        order_id = detail['orderId']
        tenant_id = detail.get('tenantId', 'pardos')
        items = detail['items']

        unavailable = []

        # 1. Chequear stock
        for item in items:
            product_id = item['productId']
            requested = int(item['qty'])  # ← FIX: Convierte a int (viene como str de JSON en EventBridge)

            resp = _get_dynamodb().get_item(
                table_name=os.environ['INVENTORY_TABLE'],
                key={'productId': product_id}
            )
            current = int(resp.get('Item', {}).get('stock', 0))  # ← BUENO: Maneja missing stock=0

            if current < requested:
                unavailable.append({
                    "productId": product_id,
                    "requested": requested,
                    "available": current
                })
                print(f"Stock insuficiente para {product_id}: {requested} req vs {current} disp")  # ← AGREGADO: Log por item

        # 2. SIN STOCK → RECHAZAR
        if unavailable:
            pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
            reason = f"Stock insuficiente: {json.dumps(unavailable)}"

            _get_dynamodb().update_item(
                table_name=os.environ['ORDERS_TABLE'],
                key={'PK': pk, 'SK': 'INFO'},
                update_expression="SET #status = :rej, rejectionReason = :reason, updatedAt = :now",
                expression_names={'#status': 'status'},
                expression_values={
                    ':rej': 'REJECTED',
                    ':reason': reason,
                    ':now': datetime.utcnow().isoformat()
                }
            )
            print("PEDIDO RECHAZADO:", reason)
            raise Exception("NO_STOCK")  # ← BUENO: Catch en StepFunctions lo maneja

        # 3. HAY STOCK → RESTAR (usa atomic update para evitar oversubtract)
        for item in items:
            product_id = item['productId']
            qty = int(item['qty'])  # ← FIX: int aquí también
            _get_dynamodb().update_item(
                table_name=os.environ['INVENTORY_TABLE'],
                key={'productId': product_id},
                update_expression="SET stock = stock - :q",
                expression_values={':q': qty}
            )
            print(f"Stock restado: {product_id} -= {qty}")  # ← AGREGADO: Log para audit

        # 4. PASAR A COOKING
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        _get_dynamodb().update_item(
            table_name=os.environ['ORDERS_TABLE'],
            key={'PK': pk, 'SK': 'INFO'},
            update_expression="SET #status = :cook, currentStep = :step, updatedAt = :now",
            expression_names={'#status': 'status'},
            expression_values={
                ':cook': 'COOKING',
                ':step': 'COOKING',  # ← SUG: ¿'PENDING_COOKING'? Para que el restaurante lo tome manual
                ':now': datetime.utcnow().isoformat()
            }
        )

        print(f"PEDIDO {order_id} APROBADO → COOKING + stock restado")
        return detail  # ← BUENO: Pasa al siguiente estado

    except KeyError as e:  # ← AGREGADO: Catch específico para missing keys en detail/items
        print(f"ERROR: Datos inválidos en event: {str(e)}")
        raise Exception("INVALID_EVENT_DATA")
    except Exception as e:
        print("ERROR FATAL:", str(e))
        raise e
    
def reject_order(event, context):
    """
    Estado final del Step Functions cuando no hay stock.
    Ahora: Recupera detalles del pedido, parsea el reason, y publica notificación a SNS
    con mensaje de rechazo, items insuficientes y reporte de stock actual.
    """
    try:
        # Log full para debug
        print(f"EVENT COMPLETO EN REJECT_ORDER: {json.dumps(event, default=str)}")
        
        # FIX: Manejo para double-nest de EventBridge en StepFunctions (detail > detail)
        detail = event.get('detail', {})
        inner_detail = detail.get('detail', detail)  # Si double, usa inner; sino, el mismo
        order_id = inner_detail.get('orderId')
        tenant_id = inner_detail.get('tenantId', 'pardos')
        
        if not order_id:
            print("ERROR: orderId no encontrado en event - abortando")
            return {"status": "REJECTED"}

        print(f"Procesando rechazo para order_id: {order_id}, tenant: {tenant_id}")
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        
        # 1. Recuperar order details (incluyendo rejectionReason e items)
        order_resp = _get_dynamodb().query(
            table_name=os.environ['ORDERS_TABLE'],
            key_condition_expression='PK = :pk AND SK = :sk',
            expression_attribute_values={':pk': pk, ':sk': 'INFO'}
        )
        order = order_resp.get('Items', [{}])[0]
        if not order:
            print("Error: Order no encontrado")
            return {"status": "REJECTED"}

        customer_id = order.get('customerId')
        rejection_reason = order.get('rejectionReason', '')
        items = order.get('items', [])
        total = float(order.get('total', 0))

        # 2. Recuperar customer email
        customer_pk = f"TENANT#{tenant_id}#CUSTOMER#{customer_id}"
        customer_resp = _get_dynamodb().get_item(
            table_name=os.environ['CUSTOMERS_TABLE'],
            key={'PK': customer_pk}
        )
        customer = customer_resp.get('Item', {})
        customer_email = customer.get('email', 'amir.ykehara@utec.edu.pe')  # Fallback a tu email

        # 3. Parsear unavailable items del rejectionReason
        unavailable_items = []
        if rejection_reason:
            try:
                start_idx = rejection_reason.find('[')
                end_idx = rejection_reason.rfind(']') + 1
                if start_idx != -1 and end_idx != -1:
                    unavailable_json = rejection_reason[start_idx:end_idx]
                    unavailable_items = json.loads(unavailable_json)
            except Exception as parse_err:
                print(f"Error parseando rejectionReason: {parse_err}")
                unavailable_items = []

        # 4. Obtener reporte completo de stock actual
        inventory_resp = _get_dynamodb().scan(table_name=os.environ['INVENTORY_TABLE'])
        current_stock = {}
        for item in inventory_resp.get('Items', []):
            product_id = item.get('productId')
            stock = int(item.get('stock', 0))
            nombre = obtener_nombre_producto(product_id) if 'obtener_nombre_producto' in globals() else product_id
            current_stock[product_id] = {'nombre': nombre, 'stock': stock}

        # 5. Publicar a SNS
        _publish_rejection_notification(
            order_id=order_id,
            customer_email=customer_email,
            customer_id=customer_id,
            total=total,
            items=items,
            unavailable_items=unavailable_items,
            current_stock=current_stock
        )

        print(f"Notificación de rechazo enviada a SNS para order {order_id} a {customer_email}")
        return {"status": "REJECTED"}

    except Exception as e:
        print(f"Error en reject_order: {e}")
        return {"status": "REJECTED"}
    
# === FUNCIONES DE DASHBOARD (DATOS REALES) ===
def obtener_resumen(event, context):
    """
    Obtiene resumen general para el dashboard - DATOS REALES
    """
    try:
        tenant_id = 'pardos'  # Por defecto
        
        # Obtener métricas REALES desde DynamoDB
        total_pedidos = obtener_total_pedidos(tenant_id)
        pedidos_hoy = obtener_pedidos_hoy(tenant_id)
        pedidos_activos = obtener_pedidos_activos(tenant_id)
        tiempo_promedio = obtener_tiempo_promedio_real(tenant_id)
        
        resumen = {
            'totalPedidos': total_pedidos,
            'pedidosHoy': pedidos_hoy,
            'pedidosActivos': pedidos_activos,
            'tiempoPromedioEntrega': tiempo_promedio,
            'ultimaActualizacion': datetime.utcnow().isoformat()
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(resumen, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def obtener_metricas(event, context):
    """
    Obtiene métricas detalladas para gráficos - DATOS REALES
    """
    try:
        tenant_id = 'pardos'
        
        metricas = {
            'pedidosPorEstado': obtener_pedidos_por_estado_real(tenant_id),
            'tiemposPorEtapa': obtener_tiempos_por_etapa_real(tenant_id),
            'pedidosUltimaSemana': obtener_pedidos_ultima_semana_real(tenant_id),
            'productosPopulares': obtener_productos_populares_real(tenant_id)
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(metricas, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def obtener_pedidos(event, context):
    """
    Obtiene lista de pedidos REALES para el dashboard - DATOS REALES
    """
    try:
        tenant_id = 'pardos'
        limit = 50
        
        # Obtener pedidos REALES desde la tabla de orders
        pedidos_reales = obtener_pedidos_reales(tenant_id, limit)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'pedidos': pedidos_reales,
                'total': len(pedidos_reales),
                'message': 'Datos reales desde DynamoDB'
            }, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# === FUNCIONES AUXILIARES PARA DASHBOARD (DATOS REALES) ===

def obtener_total_pedidos(tenant_id):
    """Obtiene el total de pedidos REALES"""
    try:
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        return response.get('Count', 0)
    except Exception as e:
        print(f"Error obteniendo total pedidos: {str(e)}")
        return 0

def obtener_pedidos_hoy(tenant_id):
    """Obtiene pedidos creados hoy - REALES"""
    try:
        hoy = datetime.utcnow().date().isoformat()
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        
        pedidos_hoy = 0
        for pedido in response.get('Items', []):
            if pedido.get('createdAt', '').startswith(hoy):
                pedidos_hoy += 1
                
        return pedidos_hoy
    except Exception as e:
        print(f"Error obteniendo pedidos hoy: {str(e)}")
        return 0

def obtener_pedidos_activos(tenant_id):
    """Obtiene pedidos activos REALES"""
    try:
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        
        activos = 0
        estados_activos = ['PENDING_VALIDATION', 'COOKING', 'PACKAGING', 'DELIVERY']
        for pedido in response.get('Items', []):
            if pedido.get('status') in estados_activos:
                activos += 1
                
        return activos
    except Exception as e:
        print(f"Error obteniendo pedidos activos: {str(e)}")
        return 0

def obtener_pedidos_por_estado_real(tenant_id):
    """Obtiene distribución REAL de pedidos por estado"""
    try:
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        
        distribucion = {
            'PENDING_VALIDATION': 0,
            'REJECTED': 0,
            'COOKING': 0,
            'PACKAGING': 0,
            'DELIVERY': 0,
            'COMPLETED': 0
        }
        
        for pedido in response.get('Items', []):
            estado = pedido.get('status', 'PENDING_VALIDATION')
            if estado in distribucion:
                distribucion[estado] += 1
            else:
                # Por si en el futuro aparece un estado nuevo (ej: CANCELLED)
                distribucion[estado] = 1
                
        return distribucion

    except Exception as e:
        print(f"Error en obtener_pedidos_por_estado_real: {str(e)}")
        # Devuelve algo razonable aunque falle
        return {
            'PENDING_VALIDATION': 0,
            'REJECTED': 0,
            'COOKING': 0,
            'PACKAGING': 0,
            'DELIVERY': 0,
            'COMPLETED': 0
        }
    
def obtener_tiempos_por_etapa_real(tenant_id):
    """Calcula tiempos REALES por etapa"""
    try:
        response = _get_dynamodb().scan(
            table_name=os.environ['STEPS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND attribute_exists(finishedAt)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#"
            }
        )
        
        tiempos = {'COOKING': [], 'PACKAGING': [], 'DELIVERY': []}
        
        for item in response.get('Items', []):
            if item.get('status') == 'COMPLETED' and item.get('finishedAt'):
                etapa = item.get('stepName')
                # FIX: Maneja typos (mapea a estándar)
                if etapa == 'DELVERIED' or etapa == 'DELVERY' or etapa == 'DELIVERIED':
                    etapa = 'DELIVERY'
                started_at = item.get('startedAt')
                finished_at = item.get('finishedAt')
                
                if etapa in tiempos and started_at and finished_at:
                    try:
                        duracion = calcular_duracion_minutos(started_at, finished_at)
                        tiempos[etapa].append(duracion)
                    except Exception as calc_err:  # FIX: Log y salta
                        print(f"Error calculando duracion para {etapa}: {calc_err}")
                        continue
        
        # Calcular promedios
        promedios = {}
        for etapa, duraciones in tiempos.items():
            if duraciones:
                promedios[etapa] = round(sum(duraciones) / len(duraciones), 1) if duraciones else 0
            else:
                promedios[etapa] = 0  # O fallback: 15 para COOKING, etc.
        
        return promedios
    except Exception as e:
        print(f"Error obteniendo tiempos por etapa: {e}")
        return {'COOKING': 15, 'PACKAGING': 5, 'DELIVERY': 25}  # Fallback

def obtener_pedidos_ultima_semana_real(tenant_id):
    """Obtiene pedidos REALES de la última semana"""
    try:
        hoy = datetime.utcnow()
        pedidos_por_dia = [0] * 7  # Últimos 7 días
        
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        
        pedidos_por_fecha = {}
        for item in response.get('Items', []):
            created_at = item.get('createdAt', '')
            if created_at:
                try:
                    fecha = datetime.fromisoformat(created_at.replace('Z', '+00:00')).date()
                    dias_diff = (hoy.date() - fecha).days
                    
                    if 0 <= dias_diff < 7:
                        order_id = item.get('orderId')
                        if order_id:
                            if fecha not in pedidos_por_fecha:
                                pedidos_por_fecha[fecha] = set()
                            pedidos_por_fecha[fecha].add(order_id)
                except:
                    continue
        
        # Organizar por día
        for i in range(7):
            fecha = hoy.date() - timedelta(days=i)
            if fecha in pedidos_por_fecha:
                pedidos_por_dia[6-i] = len(pedidos_por_fecha[fecha])
        
        return pedidos_por_dia
    except Exception as e:
        print(f"Error obteniendo pedidos última semana: {str(e)}")
        return [0, 0, 0, 0, 0, 0, 0]

def obtener_productos_populares_real(tenant_id):
    """Obtiene productos populares REALES"""
    try:
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        
        productos_count = {}
        for pedido in response.get('Items', []):
            items = pedido.get('items', [])
            for item in items:
                product_id = item.get('productId', '')
                if product_id:
                    productos_count[product_id] = productos_count.get(product_id, 0) + 1
        
        # Convertir a formato de respuesta
        productos_populares = []
        for product_id, cantidad in productos_count.items():
            nombre_producto = obtener_nombre_producto(product_id)
            productos_populares.append({
                'producto': nombre_producto,
                'cantidad': cantidad
            })
        
        # Ordenar por cantidad descendente y tomar top 3
        productos_populares.sort(key=lambda x: x['cantidad'], reverse=True)
        return productos_populares[:3]
        
    except Exception as e:
        print(f"Error obteniendo productos populares: {str(e)}")
        return [
            {'producto': 'Pollo a la Brasa', 'cantidad': obtener_total_pedidos(tenant_id)},
            {'producto': 'Chicha Morada', 'cantidad': max(obtener_total_pedidos(tenant_id) - 2, 0)},
            {'producto': 'Ensalada Fresca', 'cantidad': max(obtener_total_pedidos(tenant_id) - 5, 0)}
        ]

def obtener_nombre_producto(product_id):
    """Mapea productId a nombre de producto"""
    mapeo_productos = {
        'pollo_1_4': 'Pollo a la Brasa (1/4)',
        'pollo_1_2': 'Pollo a la Brasa (1/2)',
        'pollo_entero': 'Pollo a la Brasa (Entero)',
        'chicha': 'Chicha Morada',
        'inca_kola': 'Inca Kola',
        'ensalada': 'Ensalada Fresca'
    }
    return mapeo_productos.get(product_id, product_id)

def obtener_tiempo_promedio_real(tenant_id):
    """Calcula tiempo promedio REAL de entrega"""
    try:
        # FIX: Busca 'DELIVERY' (tus steps) en lugar de 'DELIVERED'
        response = _get_dynamodb().scan(
            table_name=os.environ['STEPS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND stepName = :step AND attribute_exists(finishedAt)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':step': 'DELIVERY'  # ← FIX: Busca 'DELIVERY' (tus datos), no 'DELIVERED'
            }
        )
        
        tiempos = []
        for item in response.get('Items', []):
            if item.get('status') == 'COMPLETED':
                order_id = item.get('orderId')
                pedido_tiempo = calcular_tiempo_total_pedido(tenant_id, order_id)
                if pedido_tiempo > 0:
                    tiempos.append(pedido_tiempo)
                    print(f"Tiempo total para order {order_id}: {pedido_tiempo} min")  # Log debug
        
        promedio = sum(tiempos) / len(tiempos) if tiempos else 45
        print(f"Tiempos encontrados: {len(tiempos)}, promedio: {promedio}")  # Log
        return round(promedio, 1) if tiempos else 45  # Float si hay datos
    except Exception as e:
        print(f"Error calculando tiempo promedio: {e}")
        return 45

def obtener_pedidos_reales(tenant_id, limit=50):
    """Obtiene lista REAL de pedidos con sus datos REALES"""
    try:
        # Obtener todos los pedidos de la tabla ORDERS
        response = _get_dynamodb().scan(
            table_name=os.environ['ORDERS_TABLE'],
            filter_expression='begins_with(PK, :pk) AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#",
                ':sk': 'INFO'
            }
        )
        
        pedidos = response.get('Items', [])
        
        # Para cada pedido, obtener sus etapas
        pedidos_completos = []
        for pedido in pedidos:
            order_id = pedido.get('orderId')
            
            # Obtener etapas del pedido
            etapas_response = _get_dynamodb().query(
                table_name=os.environ['STEPS_TABLE'],
                key_condition_expression='PK = :pk',
                expression_attribute_values={
                    ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}"
                }
            )
            etapas = etapas_response.get('Items', [])
            
            # Formatear el pedido con datos REALES
            pedido_completo = {
                'orderId': order_id,
                'customerId': pedido.get('customerId', 'N/A'),
                'status': pedido.get('status', 'CREATED'),
                'createdAt': pedido.get('createdAt', ''),
                'etapas': [],
                'items': pedido.get('items', []),  # ITEMS REALES del pedido
                'total': float(pedido.get('total', 0))  # TOTAL REAL del pedido
            }
            
            # Agregar etapas formateadas
            for etapa in etapas:
                etapa_info = {
                    'stepName': etapa.get('stepName'),
                    'status': etapa.get('status', 'IN_PROGRESS'),
                    'startedAt': etapa.get('startedAt'),
                    'finishedAt': etapa.get('finishedAt')
                }
                pedido_completo['etapas'].append(etapa_info)
            
            pedidos_completos.append(pedido_completo)
        
        # Ordenar por fecha de creación descendente
        pedidos_completos.sort(key=lambda x: x.get('createdAt', ''), reverse=True)
        
        # Aplicar límite
        pedidos_completos = pedidos_completos[:limit]
        
        return pedidos_completos
        
    except Exception as e:
        print(f"Error obteniendo pedidos reales: {str(e)}")
        return []

def calcular_duracion_minutos(inicio, fin):
    """Calcula duración en minutos entre dos timestamps (float para precisión)"""
    try:
        start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
        end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
        total_seconds = (end - start).total_seconds()
        minutos = total_seconds / 60
        return round(minutos, 1)  # ← FIX: Float con 1 decimal (ej: 0.5 min para 30s)
    except Exception as e:
        print(f"Error parseando timestamps: {e}")
        return 0

def calcular_tiempo_total_pedido(tenant_id, order_id):
    """Calcula tiempo total de un pedido desde creación hasta entrega"""
    try:
        # Obtener pedido
        pedido_response = _get_dynamodb().get_item(
            table_name=os.environ['ORDERS_TABLE'],
            key={
                'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
                'SK': 'INFO'
            }
        )
        pedido = pedido_response.get('Item', {})
        
        # Obtener todas las etapas
        etapas_response = _get_dynamodb().query(
            table_name=os.environ['STEPS_TABLE'],
            key_condition_expression='PK = :pk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}"
            }
        )
        etapas = etapas_response.get('Items', [])
        
        if not pedido or not etapas:
            return 0
        
        # Encontrar primera y última etapa
        primera_etapa = min(etapas, key=lambda x: x.get('startedAt', ''))
        ultima_etapa_completada = None
        
        for etapa in etapas:
            etapa_name = etapa.get('stepName')
            # FIX: Mapea typos a DELIVERY para contar
            if etapa_name in ['DELVERIED', 'DELVERY', 'DELIVERIED']:
                etapa_name = 'DELIVERY'
            if etapa.get('status') == 'COMPLETED' and etapa.get('finishedAt') and etapa_name == 'DELIVERY':
                if not ultima_etapa_completada or etapa.get('finishedAt') > ultima_etapa_completada.get('finishedAt', ''):
                    ultima_etapa_completada = etapa
        
        if primera_etapa.get('startedAt') and ultima_etapa_completada and ultima_etapa_completada.get('finishedAt'):
            return calcular_duracion_minutos(primera_etapa['startedAt'], ultima_etapa_completada['finishedAt'])
        
        return 0
    except Exception as e:
        print(f"Error calculando tiempo total pedido: {e}")
        return 0
                                     

# === FUNCIONES DE ETAPAS MANUALES (API para restaurante) ===
def iniciar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        assigned_to = body.get('assignedTo', 'Sistema')
        timestamp = datetime.utcnow().isoformat()
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        step_record = {
            'PK': pk,
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': 'IN_PROGRESS',
            'startedAt': timestamp,
            'assignedTo': assigned_to,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        _get_dynamodb().put_item(os.environ['STEPS_TABLE'], step_record)
        _get_dynamodb().update_item(
            table_name=os.environ['ORDERS_TABLE'],
            key={
                'PK': pk,
                'SK': 'INFO'
            },
            update_expression="SET currentStep = :step, updatedAt = :now",
            expression_values={
                ':step': stage,
                ':now': timestamp
            }
        )
        _get_events().publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'assignedTo': assigned_to,
                'timestamp': timestamp
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} iniciada',
                'stepRecord': step_record
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def completar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
        response = _get_dynamodb().query(
            table_name=os.environ['STEPS_TABLE'],
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': pk,
                ':sk': f"STEP#{stage}"
            }
        )
        if not response.get('Items'):
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Etapa no encontrada'})
            }
        latest_step = max(response['Items'], key=lambda x: x['startedAt'])
        timestamp = datetime.utcnow().isoformat()
        _get_dynamodb().update_item(
            table_name=os.environ['STEPS_TABLE'],
            key={
                'PK': latest_step['PK'],
                'SK': latest_step['SK']
            },
            update_expression="SET #s = :status, finishedAt = :finished",
            expression_names={'#s': 'status'},
            expression_values={
                ':status': 'COMPLETED',
                ':finished': timestamp
            }
        )
        
        # NUEVO: Si es DELIVERY, setea order a COMPLETED
        if stage == 'DELIVERY':
            _get_dynamodb().update_item(
                table_name=os.environ['ORDERS_TABLE'],
                key={'PK': pk, 'SK': 'INFO'},
                update_expression="SET #status = :comp, currentStep = :delivered, updatedAt = :now",
                expression_names={'#status': 'status'},
                expression_values={
                    ':comp': 'COMPLETED',
                    ':delivered': 'DELIVERED',
                    ':now': timestamp
                }
            )
            print(f"Pedido {order_id} completado: status COMPLETED, currentStep DELIVERED")
        
        _get_events().publish_event(
            source="pardos.etapas",
            detail_type="StageCompleted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'stage': stage,
                'startedAt': latest_step['startedAt'],
                'completedAt': timestamp,
                'duration': calcular_duracion(latest_step['startedAt'], timestamp)
            }
        )
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Etapa {stage} completada',
                'duration': calcular_duracion(latest_step['startedAt'], timestamp)
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# === FUNCIONES DE WORKFLOW AUTOMÁTICO (para Step Functions) ===
# ==================== WORKFLOW AUTOMÁTICO (Step Functions) ====================

def process_cooking(event, context):
    detail = event.get('detail', event)
    order_id = detail['orderId']
    tenant_id = detail.get('tenantId', 'pardos')
    pk = f"TENANT#{tenant_id}#ORDER#{order_id}"

    timestamp = datetime.utcnow().isoformat()

    # 1. Actualizamos el pedido principal
    _get_dynamodb().update_item(
        table_name=os.environ['ORDERS_TABLE'],
        key={'PK': pk, 'SK': 'INFO'},
        update_expression="SET #status = :inProgress, currentStep = :step, updatedAt = :now",
        expression_names={'#status': 'status'},
        expression_values={
            ':inProgress': 'IN_PROGRESS',  # FIX: Status "IN_PROGRESS" al entrar
            ':step': 'COOKING',
            ':now': timestamp
        }
    )

    # 2. Creamos el registro de la etapa
    _get_dynamodb().put_item(os.environ['STEPS_TABLE'], {
        'PK': pk,
        'SK': f"STEP#COOKING#{timestamp}",
        'stepName': 'COOKING',
        'status': 'IN_PROGRESS',
        'startedAt': timestamp,
        'orderId': order_id,
        'tenantId': tenant_id
    })

    print(f"Pedido {order_id} validado y puesto en cola de cocina - status IN_PROGRESS")
    return detail


def process_packaging(event, context):
    """Inicia la etapa de empaque"""
    order_id = event.get('orderId') or event['detail']['orderId']
    tenant_id = event.get('tenantId', 'pardos')
    _update_step(order_id, tenant_id, 'PACKAGING')
    return event


def process_delivery(event, context):
    """Inicia la etapa de entrega"""
    order_id = event.get('orderId') or event['detail']['orderId']
    tenant_id = event.get('tenantId', 'pardos')
    _update_step(order_id, tenant_id, 'DELIVERY')
    return event


def process_delivered(event, context):
    """Marca el pedido como COMPLETADO"""
    order_id = event.get('orderId') or event['detail']['orderId']
    tenant_id = event.get('tenantId', 'pardos')
    pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
    timestamp = datetime.utcnow().isoformat()

    _get_dynamodb().update_item(
        table_name=os.environ['ORDERS_TABLE'],
        key={'PK': pk, 'SK': 'INFO'},
        update_expression="SET #status = :comp, currentStep = :step, updatedAt = :now",
        expression_names={'#status': 'status'},
        expression_values={
            ':comp': 'COMPLETED',
            ':step': 'DELIVERED',
            ':now': timestamp
        }
    )

    # Registrar paso final
    _get_dynamodb().put_item(os.environ['STEPS_TABLE'], {
        'PK': pk,
        'SK': f"STEP#DELIVERED#{timestamp}",
        'stepName': 'DELIVERED',
        'status': 'COMPLETED',
        'startedAt': timestamp,
        'finishedAt': timestamp,
        'orderId': order_id,
        'tenantId': tenant_id
    })

    return {
        'orderId': order_id,
        'status': 'COMPLETED',
        'finalStep': 'DELIVERED'
    }

# ==================== AUXILIAR MEJORADO ====================

def _update_step(order_id, tenant_id, step):
    """
    Actualiza el currentStep y el status del pedido,
    y crea el registro de etapa en STEPS_TABLE.
    Usada por process_cooking, packaging, delivery.
    """
    pk = f"TENANT#{tenant_id}#ORDER#{order_id}"
    timestamp = datetime.utcnow().isoformat()

    # 1. Actualizamos el pedido principal
    _get_dynamodb().update_item(
        table_name=os.environ['ORDERS_TABLE'],
        key={'PK': pk, 'SK': 'INFO'},
        update_expression="SET currentStep = :step, #status = :inProgress, updatedAt = :now",
        expression_names={'#status': 'status'},
        expression_values={
            ':step': step,  # currentStep = 'COOKING'
            ':inProgress': 'IN_PROGRESS',  # status = 'IN_PROGRESS' al iniciar
            ':now': timestamp
        }
    )

    # 2. Creamos el registro de la etapa
    _get_dynamodb().put_item(os.environ['STEPS_TABLE'], {
        'PK': pk,
        'SK': f"STEP#{step}#{timestamp}",
        'stepName': step,
        'status': 'IN_PROGRESS',
        'startedAt': timestamp,
        'orderId': order_id,
        'tenantId': tenant_id
    })
# NUEVA: Funcion helper para publicar a SNS
def _publish_rejection_notification(order_id, customer_email, customer_id, total, items, unavailable_items, current_stock):
    """
    Publica mensaje JSON a SNS Topic para notificación de rechazo.
    El mensaje está listo para que un suscriptor (ej: Lambda con SES) lo use para enviar email.
    """
    # Usa el Output de CloudFormation, con fallback hardcodeado para testing
    topic_arn = os.environ.get('SnsRejectionTopicArn', 'arn:aws:sns:us-east-1:773506457076:OrderRejection-pardos-unified-dev')
    
    if not topic_arn:
        print("ERROR: No se encontró el ARN de SNS - abortando publicación")
        return
    
    # Construir mensaje legible (será el body del email)
    mensaje = f"""
Pedido Rechazado por Falta de Stock

Detalles del Pedido:
- Order ID: {order_id}
- Customer ID: {customer_id}
- Total: S/. {total:.2f}
- Items solicitados: {json.dumps(items, indent=2, default=str)}

Items con Stock Insuficiente:
{json.dumps([{u['productId']: f"Requerido: {u['requested']}, Disponible: {u['available']}"} for u in unavailable_items], indent=2) if unavailable_items else 'Ninguno (revisar logs)'}

Reporte de Stock Actual:
{json.dumps(current_stock, indent=2, default=str)}

Disculpas por las molestias. Intenta de nuevo pronto.
Pardos Unified
    """
    
    # Mensaje JSON para SNS (incluye todo para procesamiento downstream)
    sns_message = {
        'orderId': order_id,
        'customerEmail': customer_email,
        'customerId': customer_id,
        'total': total,
        'items': items,
        'unavailableItems': unavailable_items,
        'currentStockReport': current_stock,
        'rejectionMessage': mensaje.strip()  # Mensaje formateado para email
    }
    
    _get_sns().publish(
        TopicArn=topic_arn,
        Message=mensaje.strip(),  
        Subject=f"Pedido Rechazado: {order_id}",
        MessageAttributes={
            'Type': {
                'DataType': 'String',
                'StringValue': 'OrderRejection'
                },
            'JsonData': {  
                'DataType': 'String',
                'StringValue': json.dumps(sns_message, default=str)
            }
        }
    )
    print(f"Mensaje publicado a SNS Topic: {topic_arn}")
# === FUNCIONES AUXILIARES (compartidas) ===
def calcular_duracion(inicio, fin):
    start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
    end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
    return int((end - start).total_seconds())
