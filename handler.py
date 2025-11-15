import json
import boto3
import uuid
from datetime import datetime
from shared.database import DynamoDB
from shared.events import EventBridge

dynamodb = DynamoDB()
events = EventBridge()

# === FUNCIONES DEL CLIENTE (API para crear/obtener customers y orders) ===

def create_order(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        customer_id = body['customerId']
        tenant_id = body.get('tenantId', 'pardos')
        order_id = str(uuid.uuid4())  # Genera ID único y escalable
        timestamp = datetime.utcnow().isoformat()

        # Crea el registro de order metadata en DynamoDB (asumiendo estructura simple; agrega items si es necesario)
        order_metadata = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': 'METADATA',
            'orderId': order_id,
            'customerId': customer_id,
            'tenantId': tenant_id,
            'status': 'CREATED',
            'createdAt': timestamp,
            'currentStep': 'CREATED'  # Inicia en CREATED, el workflow lo moverá a COOKING
        }
        dynamodb.put_item('orders', order_metadata)

        # Publica evento con order_id para propagar al workflow (Step Functions via EventBridge)
        # Esto envía el ID automáticamente a las funciones de etapas del "restaurante"
        events.publish_event(
            source="pardos.orders",
            detail_type="OrderCreated",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'timestamp': timestamp
            }
        )

        return {
            'statusCode': 201,
            'body': json.dumps({
                'orderId': order_id,
                'message': 'Order created and workflow initiated'
            })
        }
    except Exception as e:
        print(f"Error en create_order: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_orders_by_customer(event, context):
    try:
        customer_id = event['pathParameters']['customerId']
        tenant_id = 'pardos'  # Fijo o de headers/event
        # Query eficiente: asumiendo GSI en customerId (agrega en serverless si no existe)
        # Por ahora, usa scan con filtro (no ideal para prod, pero práctico sin rehacer schema)
        response = dynamodb.scan(
            table_name='orders',
            filter_expression='customerId = :cid',
            expression_attribute_values={':cid': customer_id}
        )
        orders = [item for item in response.get('Items', []) if item.get('PK', '').startswith(f"TENANT#{tenant_id}")]
        return {
            'statusCode': 200,
            'body': json.dumps({'orders': orders})
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
            'SK': 'METADATA',
            'customerId': customer_id,
            'tenantId': tenant_id,
            'name': body.get('name'),
            'email': body.get('email'),
            'createdAt': timestamp
        }
        dynamodb.put_item('customers', customer)
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
        response = dynamodb.get_item(
            table_name='customers',
            key={'PK': f"TENANT#{tenant_id}#CUSTOMER#{customer_id}", 'SK': 'METADATA'}
        )
        item = response.get('Item')
        if not item:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Customer not found'})}
        return {
            'statusCode': 200,
            'body': json.dumps(item)
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

def get_order(event, context):
    try:
        order_id = event['pathParameters']['orderId']
        tenant_id = 'pardos'
        response = dynamodb.query(
            table_name='orders',
            key_condition_expression='PK = :pk AND SK = :sk',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
                ':sk': 'METADATA'
            }
        )
        item = response.get('Items', [{}])[0]
        if not item:
            return {'statusCode': 404, 'body': json.dumps({'error': 'Order not found'})}
        return {
            'statusCode': 200,
            'body': json.dumps(item)
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

# === FUNCIONES DE DASHBOARD (STUBS - Implementa según necesites) ===
def obtener_resumen(event, context):
    # TODO: Implementa query para resumen de orders/steps
    return {'statusCode': 200, 'body': json.dumps({'resumen': 'TODO'})}

def obtener_metricas(event, context):
    # TODO: Calcula métricas con queries (ej. avg duration)
    return {'statusCode': 200, 'body': json.dumps({'metricas': 'TODO'})}

def obtener_pedidos(event, context):
    # TODO: Query orders con filtros
    return {'statusCode': 200, 'body': json.dumps({'pedidos': 'TODO'})}

# === FUNCIONES DE ETAPAS MANUALES (API para restaurante) ===
def iniciar_etapa(event, context):
    try:
        body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        order_id = body['orderId']
        tenant_id = body['tenantId']
        stage = body['stage']
        assigned_to = body.get('assignedTo', 'Sistema')
        timestamp = datetime.utcnow().isoformat()
        step_record = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': 'IN_PROGRESS',
            'startedAt': timestamp,
            'assignedTo': assigned_to,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        dynamodb.put_item('steps', step_record)
        dynamodb.update_item(
            table_name='orders',
            key={
                'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
                'SK': 'METADATA'
            },
            update_expression="SET currentStep = :step, updatedAt = :now",
            expression_values={
                ':step': stage,
                ':now': timestamp
            }
        )
        events.publish_event(
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
        response = dynamodb.query(
            table_name='steps',
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
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
        dynamodb.update_item(
            table_name='steps',
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
        events.publish_event(
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
def cooking_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        print(f"Iniciando COOKING para orden: {order_id}")
        registrar_etapa(tenant_id, order_id, 'COOKING', 'IN_PROGRESS')
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'COOKING',
                'status': 'IN_PROGRESS',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        print(f"Cocinando pedido {order_id}...")
        return {
            'status': 'COMPLETED',
            'message': 'Cooking stage completed',
            'orderId': order_id,
            'stage': 'COOKING',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error en cooking_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def packaging_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        print(f"Iniciando PACKAGING para orden: {order_id}")
        completar_etapa_automatica(tenant_id, order_id, 'COOKING')
        registrar_etapa(tenant_id, order_id, 'PACKAGING', 'IN_PROGRESS')
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'PACKAGING',
                'status': 'IN_PROGRESS',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        print(f"Empacando pedido {order_id}...")
        return {
            'status': 'COMPLETED',
            'message': 'Packaging stage completed',
            'orderId': order_id,
            'stage': 'PACKAGING',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error en packaging_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def delivery_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        print(f"Iniciando DELIVERY para orden: {order_id}")
        completar_etapa_automatica(tenant_id, order_id, 'PACKAGING')
        registrar_etapa(tenant_id, order_id, 'DELIVERY', 'IN_PROGRESS')
        events.publish_event(
            source="pardos.etapas",
            detail_type="StageStarted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'DELIVERY',
                'status': 'IN_PROGRESS',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        print(f"Entregando pedido {order_id}...")
        return {
            'status': 'COMPLETED',
            'message': 'Delivery stage completed',
            'orderId': order_id,
            'stage': 'DELIVERY',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error en delivery_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

def delivered_stage(event, context):
    try:
        order_id = event.get('orderId')
        tenant_id = event.get('tenantId', 'pardos')
        customer_id = event.get('customerId')
        print(f"Completando DELIVERED para orden: {order_id}")
        completar_etapa_automatica(tenant_id, order_id, 'DELIVERY')
        registrar_etapa(tenant_id, order_id, 'DELIVERED', 'COMPLETED')
        actualizar_estado_final(tenant_id, order_id, 'COMPLETED')
        events.publish_event(
            source="pardos.etapas",
            detail_type="OrderCompleted",
            detail={
                'orderId': order_id,
                'tenantId': tenant_id,
                'customerId': customer_id,
                'stage': 'DELIVERED',
                'status': 'COMPLETED',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        print(f"Pedido {order_id} completado exitosamente!")
        return {
            'status': 'COMPLETED',
            'message': 'Order delivered successfully',
            'orderId': order_id,
            'stage': 'DELIVERED',
            'timestamp': datetime.utcnow().isoformat()
        }
    except Exception as e:
        print(f"Error en delivered_stage: {str(e)}")
        return {
            'status': 'FAILED',
            'error': str(e)
        }

# === FUNCIONES AUXILIARES (compartidas) ===
def registrar_etapa(tenant_id, order_id, stage, status):
    try:
        timestamp = datetime.utcnow().isoformat()
        step_record = {
            'PK': f"TENANT#{tenant_id}#ORDER#{order_id}",
            'SK': f"STEP#{stage}#{timestamp}",
            'stepName': stage,
            'status': status,
            'startedAt': timestamp,
            'tenantId': tenant_id,
            'orderId': order_id
        }
        if status == 'COMPLETED':
            step_record['finishedAt'] = timestamp
        dynamodb.put_item('steps', step_record)
        print(f"Etapa {stage} registrada para orden {order_id}")
    except Exception as e:
        print(f"Error registrando etapa: {str(e)}")

def completar_etapa_automatica(tenant_id, order_id, stage):
    try:
        response = dynamodb.query(
            table_name='steps',
            key_condition_expression='PK = :pk AND begins_with(SK, :sk)',
            expression_attribute_values={
                ':pk': f"TENANT#{tenant_id}#ORDER#{order_id}",
                ':sk': f"STEP#{stage}"
            }
        )
        if response.get('Items'):
            latest_step = max(response['Items'], key=lambda x: x['startedAt'])
            timestamp = datetime.utcnow().isoformat()
            dynamodb.update_item(
                table_name='steps',
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
            print(f"Etapa {stage} completada automaticamente")
    except Exception as e:
        print(f"Error completando etapa automatica: {str(e)}")

def actualizar_estado_final(tenant_id, order_id, status):
    print(f"Actualizando estado final del pedido {order_id} a {status}")
    # TODO: Actualiza en orders table si es necesario
    pass

def calcular_duracion(inicio, fin):
    start = datetime.fromisoformat(inicio.replace('Z', '+00:00'))
    end = datetime.fromisoformat(fin.replace('Z', '+00:00'))
    return int((end - start).total_seconds())
