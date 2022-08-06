##############################
#
# Minimus Admin - implements Admin class user interface
# for use with minimus.
#
# There is some refactoring if you wanted to use with Flask.
# must have either MontyDB (stand-alone) or PyMongo (to a MongoDB instance)
# if you expect it to do anything
#
# License - MIT License, no guarantees of suitability for your app
#
##################################
version = "0.0.2"

from minimus import Minimus, render_template, jsonify, parse_formvars, redirect, url_for, Session, abort
from montydb import MontyClient, set_storage
import json
from pymongo import MongoClient
from bson import ObjectId

import os
from passlib.context import CryptContext
import functools

pwd_context = CryptContext(
        schemes=["pbkdf2_sha256"],
        default="pbkdf2_sha256",
        pbkdf2_sha256__default_rounds=30000
)

def encrypt_password(password):
    return pwd_context.encrypt(password)

def check_encrypted_password(password, hashed):
    return pwd_context.verify(password, hashed)


class Admin:
    """
    Allow for CRUD of data in database
    TODO
    1. (done) database config for MontyDB and MongoDB remote
    2. (done) Add/Remove collection
    2. (reject) add a delete widget on each field (edit_fields)
    2a. (done) Raw JSON editing should remove used keys
    3. (done) Edit with Schema should be able to retain non-schema data (it doesn't now)
    4. user login
    5. user roles
    6. (done) Table meta info schema (use a "meta" collection and describe what should be in field and order)
	7. write up security for this, no security is included.  Possibly combine with minimus_users module.
	8. document the hell out of this module, because I will forget it.
    """
    def __init__(self, app:Minimus, 
                 session=None,
                 url_prefix="/admin",
                 db_uri=None,
                 db_file='minimus.db',
                 admin_database='minimus_admin',
                 users_collection='minimus_users',
                 require_authentication=True,
                 ):
        """__init__() - initialize the administration area"""
        global _db
        self.app = app
        self.users_collection = users_collection
        
        self.require_authentication = require_authentication
        if require_authentication:
            if session is None:
                raise ValueError("Admin() requires a 'session' to attach to if 'require_authentication' is default or True")
            self.session = session
            
        ### set up the database ###
        if db_uri:
            app.client = MongoClient(db_uri)
        else:
            set_storage(db_file)
            app.db_file = db_file
            app.client = MontyClient(db_file)
            
        app.db = app.client[admin_database]
        _db = app.db
        
        ### Add the routes ###
        app.add_route(url_prefix + '/login', self.login, methods=['GET','POST'], route_name="admin_login")
        app.add_route(url_prefix + '/logout', self.logout, route_name='admin_logout')
        ####
        app.add_route(url_prefix, self.view_all, route_name='admin_view_all')
        app.add_route(url_prefix + '/view/<coll>', self.view_collection, route_name="admin_view_collection")
        app.add_route(url_prefix + '/edit/<coll>/<id>', self.edit_fields, methods=['GET', 'POST'], route_name="admin_edit_fields")
        app.add_route(url_prefix + '/edit_schema/<coll>/<id>', self.edit_schema, methods=['GET', 'POST'], route_name="admin_edit_schema")
        app.add_route(url_prefix + '/edit_raw/<coll>/<id>', self.edit_json, methods=['GET', 'POST'], route_name="admin_edit_json")
        app.add_route(url_prefix + '/delete/<coll>', self.delete_collection_prompt, methods=['GET','POST'], route_name="admin_delete_collection")
        app.add_route(url_prefix + '/delete/<coll>/<id>', self.delete_collection_item, methods=['GET', 'POST'], route_name="admin_delete_collection_item")
        app.add_route(url_prefix + '/add/<coll>', self.add_collection_item, methods=['GET', 'POST'], route_name="admin_add_collection_item")
        app.add_route(url_prefix + '/add', self.add_mod_collection, methods=['GET','POST'], route_name="admin_add_collection")
        app.add_route(url_prefix + '/modify/<coll>', self.add_mod_collection, methods=['GET', 'POST'], route_name="admin_mod_collection")
        
        
    def login(self, env, filename=None, next=None):
        html = self.render_login()
        
        if env.get('REQUEST_METHOD') == 'POST':
            fields = parse_formvars(env)
            user = self.get_user(fields.get('username'))
            if self.authenticate(fields.get('username'), fields.get('password')):
                self.login_user(user)
                next = 'admin_view_all' if next is None else next
                return redirect(url_for(next))
        return html
    
    def login_user(self, user):
        """sets the Session"""
        self.session.connect()
        self.session.data['is_authenticated'] = True
        self.session.data['user'] = user
        self.session.save()
        
    def login_check(self):
        """login_check() - if require_authentication return user else None"""
        if self.require_authentication:
            self.session.connect()
            if self.session.data.get('is_authenticated'):
                return self.session.data['user']
            return None
        else:
            return True
        

    def logout(self, env, next=None):
        self.logout_user()
        next = next if next else '/'
        return redirect(next)
    
    def logout_user(self):
        self.session.connect()
        if 'is_authenticated' in self.session.data:
            self.session.data['is_authenticated'] = False
        if 'user' in self.session.data:   
            self.session.data.pop('user')
    
    
    def view_all(self, env):
        """view all collections in the database"""
        if not self.login_check():
            return redirect(url_for('admin_login'))
        collections = self.app.db.list_collection_names()
        return render_template('admin/view_all.html', collections=collections)
    
    def view_collection(self, env, coll):
        """view a specific collection in the database"""
        if not self.login_check():
            return redirect(url_for('admin_login'))        
        data = list(self.app.db[coll].find())
        schema = self.app.db['_meta'].find_one({'name':coll})
        for doc in data:
            doc['_id'] = str(doc['_id'])
        #if len(data) == 0:
        #    return jsonify({'status': 'error', 'message': 'no data'})
        return render_template('admin/view_collection.html', coll=coll, data=data, schema=schema)
            
    def edit_fields(self, env, coll, id):
        """render a specific record as fields
		** combine with edit_schema() during refactor
		"""
        if not self.login_check():
            return abort(401)        
        try:
            key = {'_id': ObjectId(id)}
        except Exception as e:
            return jsonify({'status': 'error', 'message': 'Admin edit_fields(), ' + str(e)})
        
        if env.get('REQUEST_METHOD') == 'POST':
            # write the data
            try:
                old_data = self.app.db[coll].find_one(key)
                data = parse_formvars(env)
                if '_id' in data:
                    data.pop('_id')
                if 'csrf_token' in data:
                    data.pop('csrf_token')
                self.app.db[coll].update_one(key, {'$set': data})
                data['_id'] = id
                return redirect(url_for('admin_view_collection', coll=coll))
            except Exception as e:
                return jsonify({'status': 'error', 'message': 'Admin edit_fields(), ' + str(e)})
        else:
            # view the data
            try:
                data = self.app.db[coll].find_one(key)
                data['_id'] = str(data['_id'])
                fields = fields_transform(data)
                return render_template('admin/edit_fields.html', coll=coll, fields=fields, id=data['_id'])
            except Exception as e:
                return jsonify({'status': 'error', 'message': 'Admin edit_fields(), ' + str(e)})
    
    
    def edit_json(self, env, coll, id):
        """render a specific record as JSON"""
        if not self.login_check():
            return abort(401)        
        try:
            key = {'_id': ObjectId(id)}
            data = self.app.db[coll].find_one(key)
        except Exception as e:
            return jsonify({'status': 'error', 'message': 'Admin edit_json(), ' + str(e)})
        
        if env.get('REQUEST_METHOD') == 'POST':
            try:
                raw = parse_formvars(env)
                text_format = raw.get('content')
                data = json.loads(text_format)
                #self.app.db[coll].update_one(key, {'$set': data})
                self.app.db[coll].replace_one(key, data)
                return redirect(url_for('admin_view_collection', coll=coll))
            except Exception as e:
                return jsonify({'status': 'error', 'message': 'Admin edit_json, ' + str(e)})
        else:
            # render the JSON
            if '_id' in data:
                data.pop('_id')
            return render_template('admin/edit_json.html', coll=coll, content=json.dumps(data), error=None)
        
    def edit_schema(self, env, coll, id):
        """edit collection item with based on a schema
        env - the environment
        coll - collection name
        id - the database id
        """
        if not self.login_check():
            return abort(401)        
        try:
            key = {'_id': ObjectId(id)}
        except Exception as e:
            return jsonify({'status': 'error', 'message': 'Admin edit_schema(), ' + str(e)})
        
        if env.get('REQUEST_METHOD') == 'POST':
            # write the data
            try:
                old_data = self.app.db[coll].find_one(key)
                data = parse_formvars(env)
                if '_id' in data:
                    data.pop('_id')
                if 'csrf_token' in data:
                    data.pop('csrf_token')
                data = dict(data)
                #self.app.db[coll].replace_one(key, data) # replace_one kills any data not in schema!
                self.app.db[coll].update_one(key, {'$set':data})
                data['_id'] = id
                return redirect(url_for('admin_view_collection', coll=coll))
            except Exception as e:
                return jsonify({'status': 'error', 'message': 'Admin edit_schema(), ' + str(e)})
        else:
            # view the data
            try:
                schema = self.app.db['_meta'].find_one({'name':coll})
                data = self.app.db[coll].find_one(key)
                fields = schema_transform(data, schema)
                data['_id'] = str(data['_id'])
                return render_template('admin/edit_schema.html', coll=coll, fields=fields, id=data['_id'])
            except Exception as e:
                return jsonify({'status': 'error', 'message': 'Admin edit_schema(), ' + str(e)})
        
    def add_collection_item(self, env, coll):
        """Add a new item to the collection, raw JSON"""
        if not self.login_check():
            return abort(401)        
        if env.get('REQUEST_METHOD') == 'GET':    
            return render_template('admin/add_json.html', coll=coll)
        else:
            fields = parse_formvars(env)
            raw = fields.get('content')
            try:
                data = json.loads(raw)
            except:
                data = cook_data(raw)
            self.app.db[coll].insert_one(data)
            data['_id'] = str(data['_id'])
        return redirect(url_for('admin_view_collection', coll=coll))
    
    def add_mod_collection(self, env, coll=None):
        """Add or Modify a collection"""
        if not self.login_check():
            return abort(401)        
        fields = {}
        key = None
        if coll:
            # find record of schema
            fields['name'] = coll
            rec = self.app.db['_meta'].find_one({'name':coll})
            if rec:
                key = {'_id': rec['_id']}
                fields['schema'] = rec['schema']
            
        if env.get('REQUEST_METHOD') == 'POST':
            fields = parse_formvars(env)
            name = fields.get('name')
            if name is None:
                return redirect( url_for('admin_view_all') )
            
            schema = fields.get('schema')
            meta = {'name': name, 'schema': schema}
            if schema:
                if key:
                    # since it exists, replace
                    self.app.db['_meta'].replace_one(key, meta)
                else:
                    # it's new insert
                    self.app.db['_meta'].insert_one(meta)
                
            # create the collection if it doesn't exist
            if not name in self.app.db.list_collection_names():
               id = self.app.db[name].insert_one({}).inserted_id
               self.app.db[name].delete_one({'_id':id})
            
            return redirect(url_for('admin_view_all'))
        
        return render_template('admin/add_mod_collection.html', fields=fields)
    
    def delete_collection_item(self, env, coll, id):
        if not self.login_check():
            return abort(401)        
        try:
            key = {'_id': ObjectId(id)}
            old_data = self.app.db[coll].find_one(key)
        except Exception as e:
            return jsonify({'status': 'error', 'message': 'deleteJSON non-existent id, ' + str(e)})
    
        self.app.db[coll].delete_one(key)
        return redirect(url_for('admin_view_collection', coll=coll))
    
    def delete_collection_prompt(self, env, coll):
        """delete collection with prompt"""
        if not self.login_check():
            return abort(401)        
        fields = {}
        if env.get('REQUEST_METHOD') == 'POST':
            fields = parse_formvars(env)
            if fields.get('name') == coll and fields.get('agree') == 'on':
                self.app.db[coll].drop()
            return redirect(url_for('admin_view_all'))
                
        return render_template('admin/delete_collection_prompt.html', fields=fields, coll=coll)
    
    def delete_collection(self, env, coll):
        """DANGER -- this method will delete a collection immediately"""
        if not self.login_check():
            return abort(401)        
        self.app.db[coll].drop()
        return redirect(url_for('admin_view_all'))
    
    def unit_tests(self):
        """simple test of connectivity.  more tests should be included in separate module"""
        name = '__test_collection'
        _id = self.app.db[name].insert_one({}).inserted_id
        names = self.app.db.list_collection_names()
        assert(name in names)
        self.app.db[name].drop()
        print("*** All tests passed ***")
        
    def get_users(self):
        """get_users() - return a list of all users JSON records"""
        if _db is None:
            raise ValueError("Database not initialized!")
        return list(_db[self.users_collection].find())
    
    def get_user(self, username=None, uid=None):
        """get_user(username, uid) ==> find a user record by uid or username
        : param {username} : - a specific username (string)
        : param {uid} : - a specific user id (string) - note, this is actual '_id' in databse
        : return : a user record or None if not found
        """
        if _db is None:
            raise ValueError("Database not initialized!")
        # first try the username--
        user = None
        if username:
            user = _db[self.users_collection].find_one({'username': username})
        if uid:
            user = _db[self.users_collection].find_one({'_id':uid})
        return user
    
    def create_user(self, username, password, **kwargs):
        """
        create_user(username, password, **kwargs) ==> create a user --
        : param {username} and param {password} : REQUIRED
        : param **kwargs : python style (keyword arguments, optional)
        : return : Boolean True if user successfully created, False if exisiting username
        example
        create_user('joe','secret',display_name='Joe Smith',is_editor=True)
        """
        user = self.get_user(username=username)
        if user:
            # user exists, return failure
            return False
        # build a user record from scratch
        user = {'username':username, 'password': encrypt_password(password)}
        for key, value in kwargs.items():
            user[key] = value
    
        _db[self.users_collection].insert_one(user)
        return True
    
    def update_user(self, username, **kwargs):
        """
        update_user(username, **kwargs) - update a user record with keyword arguments
        : param {username} : an existing username in the database
        : param **kwargs : Python style keyword arguments.
        : return : True if existing username modified, False if no username exists.
        update a user with keyword arguments
        return True for success, False if fails
        if a keyword argument is EXPLICITLY set to None,
        the argument will be deleted from the record.
        NOTE THAT TinyMongo doesn't implement $unset
        """
        user = self.get_user(username)
        if user:
            idx = {'_id': user['_id']}
            for key, value in kwargs.items():
                if value is None and key in user:
                    # delete the key
                    _db[self.users_collection].update_one(idx, {'$unset': {key:""}} )
                else:
                   # user[key] = value
                   _db[self.users_collection].update_one(idx, {'$set': {key:value}} )
            return True
        return False
    
    def delete_user(self, username=None, uid=None):
        """delete_user(username, uid) deletes a user record by username or uid
        : param {username} : string username on None
        : param {uid} : string database id or None
        : return : returns user record upon success, None if fails
        """
        user = None
        if username:
            user = self.get_user(username=username)
        if uid:
            user = self.get_user(uid=uid)
        if user:
            _db[self.users_collection].remove(user)
        return user
    
    def authenticate(self, username, password):
        """
        authenticate(username, password) ==> authenticate username, password against datastore
        : param {username} : string username
        : param {password} : string password in plain-text
        : return : Boolean True if match, False if no match
        """
        user = self.get_user(username)
        if user:
            if check_encrypted_password(password, user['password']):
                return True
        return False
    
    def render_login(self, login_filename=None):
        """render_login(login_filename=None) returns a login page as a string contained
        login_file if None, then if loads module level file login.html
        : param {login_filename} : string of filename of login page HTML document or None.
        If None, then the package level standard login.html is loaded.
        : return : string HTML of login page
        NOTE: this is an experimental feature
        """
        # use module level 'login.html''
        if login_filename is None:
            moduledir = os.path.dirname(__file__)
            login_filename = os.path.join(moduledir, 'login.html')
        if not isinstance(login_filename, str):
            raise TypeError("ERROR: minmus_users.login_page() - login_filename must be a string")
        with open(login_filename) as fp:
            data = fp.read()
        return data
    
    def user_services_cli(self, args):
        """command line interface for user services"""
        if '--createuser' in args:
            username = input('Username (required): ')
            realname = input('Real Name: ')
            email = input('Email: ')
            password = input('Password (required):')
            self.create_user(username, password, realname=realname, email=email)
            return True
            
        if '--deleteuser' in args:
            username = input('Username (required): ')
            self.delete_user(username)
            return True
            
        if '--listusers' in args:
            users = self.get_users()
            for user in users:
                print(user)
            return True
        
        if '--updateuser' in args:
            username = input('Username (required): ')
            realname = input('Real Name: ')
            email = input('Email: ')
            password = input('Password (required):')
            self.update_user(username, password, realname=realname, email=email)
            return True
        
        if len(args) > 1:
            print('user services:')
            print('  --createuser')
            print('  --deleteuser')
            print('  --listusers')
            print('  --updateuser')
            return True
            
        return False    
    
    
    
def schema_transform(data, schema):
    """schema_transform() - create fields from data document and schema
    data - the document data
    schema - the document schema
    """
    # grab the schema buffer
    schema_lines = schema.get('schema').split('\n')
    fields = []
    for line in schema_lines:
        if line:
            field = {}
            parts = line.split(':') # break it on ':'
            field['name'] = parts[0].strip() # the name part
            subparts = parts[1].strip().split(' ') # split it on spaces
            field['type'] = subparts[0].strip() # get the type
            if len(subparts) > 2:
                field['label'] = ' '.join(subparts[1:])
            else:
                field['label'] = field['name'].title()
            # if value is missing, make it an empty string
            field['value'] = data.get(field['name'], '')
            fields.append(field)
    return fields
    
    
def fields_transform(fields):
    """transform fields to be used in form"""
    nfields = []
    for key, value in fields.items():
        nf = {}
        nf['name'] = key
        nf['value'] = str(value)
        nf['label'] = key.capitalize()
        if '\n' in nf['value']:
            nf['type'] = 'textarea'
        else:
            nf['type'] = 'text'
        nfields.append(nf)
    return nfields

def cook_data(raw_data):
    """cook data to be used in form"""
    data = {}
    lines = raw_data.split('\n')
    for line in lines:
        if ':' in line:
            key, value = line.split(':')
            key = key.strip()
            data[key] = value.strip()
    return data

if __name__ == '__main__':
    print(f"Minimus Admin - VERSION {version}")
    print("... Minimus Admin is not intended for direct execution. ...")
    app = Minimus(__name__)
    admin = Admin(app, require_authentication=False)
    admin.unit_tests()
    print("done")
