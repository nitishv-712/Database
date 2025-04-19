import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;

void main() {
  runApp(DatabaseManagerApp());
}

class DatabaseManagerApp extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Database Manager',
      theme: ThemeData(primarySwatch: Colors.blue),
      home: DefaultTabController(
        length: 3,
        child: Scaffold(
          appBar: AppBar(
            title: Text('Database Manager'),
            bottom: TabBar(
              tabs: [
                Tab(text: 'User'),
                Tab(text: 'Collection'),
                Tab(text: 'Document'),
              ],
            ),
          ),
          body: TabBarView(
            children: [UserTab(), CollectionTab(), DocumentTab()],
          ),
        ),
      ),
    );
  }
}

const String baseUrl = 'http://localhost:8080';

/// ====================
/// USER TAB
/// ====================
class UserTab extends StatefulWidget {
  @override
  _UserTabState createState() => _UserTabState();
}

class _UserTabState extends State<UserTab> {
  final TextEditingController _usernameController = TextEditingController();
  String _message = '';

  Future<void> _createUser() async {
    final username = _usernameController.text.trim();
    if (username.isEmpty) {
      setState(() => _message = 'Please enter a username.');
      return;
    }
    final url = Uri.parse('$baseUrl/user/$username');
    try {
      final response = await http.post(url);
      if (response.statusCode == 200) {
        setState(() => _message = 'User created: $username');
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(16.0),
      child: Column(
        children: [
          TextField(
            controller: _usernameController,
            decoration: InputDecoration(labelText: 'Username'),
          ),
          SizedBox(height: 16),
          ElevatedButton(onPressed: _createUser, child: Text('Create User')),
          SizedBox(height: 16),
          Text(_message),
        ],
      ),
    );
  }
}

/// ====================
/// COLLECTION TAB
/// ====================
class CollectionTab extends StatefulWidget {
  @override
  _CollectionTabState createState() => _CollectionTabState();
}

class _CollectionTabState extends State<CollectionTab> {
  final TextEditingController _usernameController = TextEditingController();
  final TextEditingController _collectionController = TextEditingController();
  String _message = '';
  List<String> _collections = [];

  Future<void> _createCollection() async {
    final username = _usernameController.text.trim();
    final collectionName = _collectionController.text.trim();
    if (username.isEmpty || collectionName.isEmpty) {
      setState(
        () => _message = 'Please enter both username and collection name.',
      );
      return;
    }
    final url = Uri.parse('$baseUrl/user/$username/collection/$collectionName');
    try {
      final response = await http.post(url);
      if (response.statusCode == 200) {
        setState(() => _message = 'Collection created: $collectionName');
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  Future<void> _listCollections() async {
    final username = _usernameController.text.trim();
    if (username.isEmpty) {
      setState(() => _message = 'Please enter a username.');
      return;
    }
    final url = Uri.parse('$baseUrl/user/$username/collections');
    try {
      final response = await http.get(url);
      if (response.statusCode == 200) {
        List<dynamic> data = json.decode(response.body);
        setState(() {
          _collections = data.map((e) => e.toString()).toList();
          _message = 'Collections loaded';
        });
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  @override
  Widget build(BuildContext context) {
    return SingleChildScrollView(
      padding: const EdgeInsets.all(16.0),
      child: Column(
        children: [
          TextField(
            controller: _usernameController,
            decoration: InputDecoration(labelText: 'Username'),
          ),
          TextField(
            controller: _collectionController,
            decoration: InputDecoration(labelText: 'Collection Name'),
          ),
          SizedBox(height: 16),
          ElevatedButton(
            onPressed: _createCollection,
            child: Text('Create Collection'),
          ),
          SizedBox(height: 16),
          ElevatedButton(
            onPressed: _listCollections,
            child: Text('List Collections'),
          ),
          SizedBox(height: 16),
          Text(_message),
          SizedBox(height: 16),
          _collections.isNotEmpty
              ? Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children:
                    _collections
                        .map(
                          (coll) =>
                              Text('- $coll', style: TextStyle(fontSize: 16)),
                        )
                        .toList(),
              )
              : Container(),
        ],
      ),
    );
  }
}

/// ====================
/// DOCUMENT TAB
/// ====================
class DocumentTab extends StatefulWidget {
  @override
  _DocumentTabState createState() => _DocumentTabState();
}

class _DocumentTabState extends State<DocumentTab> {
  final TextEditingController _usernameController = TextEditingController();
  final TextEditingController _collectionController = TextEditingController();
  final TextEditingController _docJsonController = TextEditingController();
  final TextEditingController _fieldController = TextEditingController();
  String _message = '';
  List<dynamic> _documents = [];

  Future<void> _insertDocument() async {
    final username = _usernameController.text.trim();
    final collectionName = _collectionController.text.trim();
    final docJson = _docJsonController.text.trim();
    if (username.isEmpty || collectionName.isEmpty || docJson.isEmpty) {
      setState(
        () =>
            _message =
                'Please fill in username, collection name, and document JSON.',
      );
      return;
    }
    final url = Uri.parse(
      '$baseUrl/user/$username/collection/$collectionName/document/list',
    );
    try {
      final response = await http.post(
        url,
        body: docJson,
        headers: {'Content-Type': 'application/json'},
      );
      if (response.statusCode == 200) {
        setState(() => _message = 'Document inserted.');
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  Future<void> _listDocuments() async {
    final username = _usernameController.text.trim();
    final collectionName = _collectionController.text.trim();
    if (username.isEmpty || collectionName.isEmpty) {
      setState(() => _message = 'Please enter username and collection name.');
      return;
    }
    final url = Uri.parse(
      '$baseUrl/user/$username/collection/$collectionName/documents',
    );
    try {
      final response = await http.get(url);
      if (response.statusCode == 200) {
        List<dynamic> data = json.decode(response.body);
        setState(() {
          _documents = data;
          _message = 'Documents loaded';
        });
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  Future<void> _countDocuments() async {
    final username = _usernameController.text.trim();
    final collectionName = _collectionController.text.trim();
    if (username.isEmpty || collectionName.isEmpty) {
      setState(() => _message = 'Please enter username and collection name.');
      return;
    }
    final url = Uri.parse(
      '$baseUrl/user/$username/collection/$collectionName/count',
    );
    try {
      final response = await http.get(url);
      if (response.statusCode == 200) {
        var data = json.decode(response.body);
        setState(() => _message = 'Count: ${data["count"]}');
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  Future<void> _sumField() async {
    final username = _usernameController.text.trim();
    final collectionName = _collectionController.text.trim();
    final field = _fieldController.text.trim();
    if (username.isEmpty || collectionName.isEmpty || field.isEmpty) {
      setState(
        () => _message = 'Please enter username, collection name, and field.',
      );
      return;
    }
    final url = Uri.parse(
      '$baseUrl/user/$username/collection/$collectionName/sum?field=$field',
    );
    try {
      final response = await http.get(url);
      if (response.statusCode == 200) {
        var data = json.decode(response.body);
        setState(() => _message = 'Sum of "$field": ${data["sum"]}');
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  Future<void> _distinctValues() async {
    final username = _usernameController.text.trim();
    final collectionName = _collectionController.text.trim();
    final field = _fieldController.text.trim();
    if (username.isEmpty || collectionName.isEmpty || field.isEmpty) {
      setState(
        () => _message = 'Please enter username, collection name, and field.',
      );
      return;
    }
    final url = Uri.parse(
      '$baseUrl/user/$username/collection/$collectionName/distinct?field=$field',
    );
    try {
      final response = await http.get(url);
      if (response.statusCode == 200) {
        List<dynamic> data = json.decode(response.body);
        setState(() => _message = 'Distinct values: ${data.join(", ")}');
      } else {
        setState(() => _message = 'Error: ${response.body}');
      }
    } catch (e) {
      setState(() => _message = 'Request failed: $e');
    }
  }

  Widget _buildDocumentList() {
    if (_documents.isEmpty) return Text('No documents to display.');
    return ListView.builder(
      shrinkWrap: true,
      itemCount: _documents.length,
      itemBuilder: (context, index) {
        return ListTile(title: Text(_documents[index].toString()));
      },
    );
  }

  @override
  Widget build(BuildContext context) {
    return SingleChildScrollView(
      padding: const EdgeInsets.all(16.0),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          TextField(
            controller: _usernameController,
            decoration: InputDecoration(labelText: 'Username'),
          ),
          TextField(
            controller: _collectionController,
            decoration: InputDecoration(labelText: 'Collection Name'),
          ),
          SizedBox(height: 16),
          TextField(
            controller: _docJsonController,
            decoration: InputDecoration(
              labelText: 'Document JSON (e.g., {"key": "value"})',
            ),
            maxLines: 3,
          ),
          SizedBox(height: 16),
          Row(
            children: [
              ElevatedButton(
                onPressed: _insertDocument,
                child: Text('Insert Document'),
              ),
              SizedBox(width: 16),
              ElevatedButton(
                onPressed: _listDocuments,
                child: Text('List Documents'),
              ),
            ],
          ),
          SizedBox(height: 16),
          Row(
            children: [
              ElevatedButton(onPressed: _countDocuments, child: Text('Count')),
              SizedBox(width: 16),
              ElevatedButton(onPressed: _sumField, child: Text('Sum Field')),
              SizedBox(width: 16),
              ElevatedButton(
                onPressed: _distinctValues,
                child: Text('Distinct'),
              ),
            ],
          ),
          SizedBox(height: 16),
          TextField(
            controller: _fieldController,
            decoration: InputDecoration(labelText: 'Field (for Sum/Distinct)'),
          ),
          SizedBox(height: 16),
          Text(
            _message,
            style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
          ),
          Divider(),
          _buildDocumentList(),
        ],
      ),
    );
  }
}
