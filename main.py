from uuid import uuid4
import boto3
from boto3.dynamodb.conditions import Key

# Conexión con la base de datos
def get_db_table():
    dynamodb_resource = boto3.resource("dynamodb")

    return dynamodb_resource.Table("siobhan-academia")

# Cuentas nuevas
def register_account(user_email: str, user_name: str) -> dict:
    ddb_table = get_db_table()

    response = ddb_table.put_item(
        Item={
            "PK": user_email,
            "SK": "PROFILE#",
            "name": user_name,
        }
    )

    return response

# Registrar cuentas invitadas de las cuentas SK
def invite_account(user_email: str, invited_user_email: str, invited_user_name: str):
    ddb_table = get_db_table()

    response = ddb_table.put_item(
        Item={
            "PK": user_email,
            "SK": f"USER#{invited_user_email}",
            "name": invited_user_name,
        }
    )

    return response

# Registrar objetos
def register_inventory(user_email: str, inventory_name: str, inventory_price: int):
    ddb_table = get_db_table()

    response = ddb_table.put_item(
        Item={
            "PK": user_email,
            "SK": f"INVENTORY#{str(uuid4())}",
            "name": inventory_name,
            "price": inventory_price,
        }
    )

    return response

# Imprimir inventario dependiendo del email PK
def get_inventory(user_email: str):
    ddb_table = get_db_table()

    response = ddb_table.query(
        KeyConditionExpression=Key("PK").eq(user_email)
        & Key("SK").begins_with("INVENTORY#")
    )

    return response["Items"]

# Imprimir invitados dependiendo del email PK
def get_invited_users(user_email: str):
    ddb_table = get_db_table()

    response = ddb_table.query(
        KeyConditionExpression=Key("PK").eq(user_email) & Key("SK").begins_with("USER#")
    )

    return response["Items"]

# Eliminar un objeto de inventario de un usuario PK por el ID
def delete_inventory(user_email: str, inventory_id: str):
    ddb_table = get_db_table()

    response = ddb_table.delete_item(
        Key={
            "PK": user_email,
            "SK": f"INVENTORY#{inventory_id}",
        }
    )

    return response

# Actualizar los datos de inventario
def update_inventory(
    user_email: str,
    inventory_id: str,
    new_inventory_name: str,
    new_inventory_price: int,
):
    ddb_table = get_db_table()

    response = ddb_table.put_item(
        Item={
            "PK": user_email,
            "SK": f"INVENTORY#{inventory_id}",
            "name": new_inventory_name,
            "price": new_inventory_price,
        }
    )

    return response

# Actualizar una cuenta pk
def update_account(
    user_email: str,
    new_user_email: str,
    new_name: str,
):
    ddb_table = get_db_table()

    # Actualizar elemento con SK "PROFILE#"
    response_profile = ddb_table.get_item(
        Key={
            "PK": user_email,
            "SK": "PROFILE#",
        }
    )
    
    item_profile = response_profile.get("Item")
    
    if item_profile:
        ddb_table.delete_item(
            Key={
                "PK": user_email,
                "SK": "PROFILE#",
            }
        )

        ddb_table.put_item(
            Item={
                "PK": new_user_email,
                "SK": "PROFILE#",
                "name": new_name,
            }
        )
    
    # Actualizar elementos con SK "INVENTORY#"
    response_inventory = ddb_table.query(
        KeyConditionExpression=Key("PK").eq(user_email) & Key("SK").begins_with("INVENTORY#")
    )
    
    items_inventory = response_inventory.get("Items")
    
    for item in items_inventory:
        ddb_table.update_item(
            Key={
                "PK": user_email,
                "SK": item["SK"],
            },
            UpdateExpression=f"SET PK = '{new_user_email}'",
        )
    
    # Actualizar elementos con SK "USER#"
    response_users = ddb_table.query(
        KeyConditionExpression=Key("PK").eq(user_email) & Key("SK").begins_with("USER#")
    )
    
    items_users = response_users.get("Items")
    
    for item in items_users:
        ddb_table.update_item(
            Key={
                "PK": user_email,
                "SK": item["SK"],
            },
            UpdateExpression=f"SET PK = '{new_user_email}'",
        )
    
    return response_profile, response_inventory, response_users


# Imprimir los perfiles PK para comprobar los cambios
def get_users_with_profile():
    ddb_table = get_db_table()

    response = ddb_table.scan(
        FilterExpression=Key("SK").begins_with("PROFILE#")
    )

    return response["Items"]

# Eliminar toda una PK usando una query
def delete_pk(user_email: str):
    ddb_table = get_db_table()

    response = ddb_table.query(
        KeyConditionExpression=Key("PK").eq(user_email)
    )

    # Sino hay algo regresa una lista vacía []
    items = response.get("Items", [])

    for item in items:
        ddb_table.delete_item(
            Key={
                "PK": item["PK"],
                "SK": item["SK"]
            }
        )

    return response

# <---------------------------------------->

# print(delete_pk(
#     user_email="joananuevo@dominio.com"
# ))

# <---------------------------------------->

# print(get_users_with_profile())

# print(update_account(
#     user_email="joana@dominio.com",
#     new_user_email="joananuevo@dominio.com",
#     new_name= "Joana Madai"
# ))

# print(get_users_with_profile())


# <---------------------------------------->
# print(register_account(
#     user_email="joana@dominio.com",
#     user_name="Joana Pineda"
# ))
# Registré Siobhan y Joana


# <---------------------------------------->

# print(invite_account(
#     user_email="joana@dominio.com",
#     invited_user_email="joana2@dominio.com",
#     invited_user_name="Joana Pineda 2",
# ))


# for item in get_invited_users(user_email="joana@dominio.com"):
#     print(item)

# <---------------------------------------->

# print(register_inventory(
#     user_email="joana@dominio.com",
#     inventory_name="Blusa",
#     inventory_price=150,
# ))

# for item in get_inventory(user_email="joana@dominio.com"):
#     print(item)

# <---------------------------------------->

# print(
#     delete_inventory(
#         user_email="siobhan@dominio.com",
#         inventory_id="5400cd30-091e-45a9-a353-fd80a830ef27",
#     )
# )

# <---------------------------------------->

# print(update_inventory(
#     user_email="siobhan@dominio.com",
#     inventory_id="f60f9ac8-896e-4388-a730-c5171f7f1a22",
#     new_inventory_name="Takis fuego",
#     new_inventory_price=23,
# ))