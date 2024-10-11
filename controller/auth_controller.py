from flask import Blueprint, request, jsonify, make_response
from flask_jwt_extended import create_access_token, jwt_required

auth_bp = Blueprint('auth_bp', __name__)

# Instancia del servicio de autenticación
users = {
    "usuario1": {"password": "password1"},
    "usuario2": {"password": "password2"}
}

# Ruta de autenticación
@auth_bp.route('/api/v1/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400

    username = request.json.get("username", None)
    password = request.json.get("password", None)

    if not username or not password:
        return jsonify({"msg": "Missing username or password"}), 400

    user = users.get(username)
    if user and user["password"] == password:
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({"msg": "Bad username or password"}), 401

# Ruta protegida que devuelve texto plano
@auth_bp.route('/api/v1/protected', methods=['GET'])
@jwt_required()
def protected():
    # Llamar al servicio para obtener el texto plano
    plain_text_data = """\
        Nombre,Apellido_p,Apellido_m,Año,Email
        Dario,rojas,saavedra,2015,dario@gmail.com
        Estefany,Carrillo,Live,2010,estefany@gmail.com
        Karina,peche,aguilar,2000,karina@gmail.com
    """

    response = make_response(plain_text_data)
    response.headers['Content-Type'] = 'text/plain'
    return response
