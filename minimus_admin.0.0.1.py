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
version = 0.0.1

from minimus import Minimus, render_template, jsonify, parse_formvars, redirect, url_for
from montydb import MontyClient, set_storage
import json
from pymongo import MongoClient
from bson import ObjectId


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
    def __init__(self, app:Minimus, url_prefix="/admin", db_uri=None, db_file='minimus.db', admin_database='minimus_admin'):
        self.app = app
        
        ### set up the database ###
        
        if db_uri:
            app.client = MongoClient(db_uri)
        else:
            set_storage(db_file)
            app.db_file = db_file
            app.client = MontyClient(db_file)
            
        app.db = app.client[admin_database]
        
        ### Add the routes ###
        app.add_route(url_prefix + '/login', self.login)
        app.add_route(url_prefix + '/logout', self.logout)
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
        
        
    def login(self, env):
        return "Login view, override this"
    
    def logout(self, env):
        return "Logout view, override this"
    
    
    def view_all(self, env):
        """view all collections in the database"""
        collections = self.app.db.list_collection_names()
        return render_template('admin/view_all.html', collections=collections)
    
    def view_collection(self, env, coll):
        """view a specific collection in the database"""
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
        try:
            key = {'_id': ObjectId(id)}
            old_data = self.app.db[coll].find_one(key)
        except Exception as e:
            return jsonify({'status': 'error', 'message': 'deleteJSON non-existent id, ' + str(e)})
    
        self.app.db[coll].delete_one(key)
        return redirect(url_for('admin_view_collection', coll=coll))
    
    def delete_collection_prompt(self, env, coll):
        """delete collection with prompt"""
        fields = {}
        if env.get('REQUEST_METHOD') == 'POST':
            fields = parse_formvars(env)
            if fields.get('name') == coll and fields.get('agree') == 'on':
                self.app.db[coll].drop()
            return redirect(url_for('admin_view_all'))
                
        return render_template('admin/delete_collection_prompt.html', fields=fields, coll=coll)
    
    def delete_collection(self, env, coll):
        """DANGER -- this method will delete a collection immediately"""
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
    
def schema_transform(data, schema):
    """create fields from data document and schema
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
    admin = Admin(app)
    admin.unit_tests()
    print("done")
