#include <unordered_map>
#include <vector>
#include <string>
#include <set>
#include <optional>
#include <cstdlib>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>
#include <mutex>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <arpa/inet.h>

using namespace std;

// ------------------------- Data Structures -------------------------

using Document = unordered_map<string, string>;

// === Collection: stores documents ===
class Collection {
public:
    void insert(Document doc) {
        doc["_id"] = to_string(nextId++);
        documents.push_back(doc);
    }

    vector<Document> findAll() const {
        return documents;
    }

    optional<Document> findById(const string& id) const {
        for (const auto& doc : documents) {
            if (doc.at("_id") == id) {
                return doc;
            }
        }
        return nullopt;
    }

    bool deleteById(const string& id) {
        for (auto it = documents.begin(); it != documents.end(); ++it) {
            if (it->at("_id") == id) {
                documents.erase(it);
                return true;
            }
        }
        return false;
    }

    int sum(const string& key) const {
        int total = 0;
        for (const auto& doc : documents) {
            if (doc.find(key) != doc.end()) {
                total += atoi(doc.at(key).c_str());
            }
        }
        return total;
    }

    set<string> distinct(const string& key) const {
        set<string> values;
        for (const auto& doc : documents) {
            if (doc.find(key) != doc.end()) {
                values.insert(doc.at(key));
            }
        }
        return values;
    }

    int countDocuments() const {
        return documents.size();
    }

private:
    vector<Document> documents;
    int nextId = 1;
};

// === UserDB: user has multiple collections ===
class UserDB {
public:
    void createCollection(const string& collectionName) {
        if (collections.find(collectionName) == collections.end()) {
            collections[collectionName] = Collection();
        }
    }

    Collection& getCollection(const string& name) {
        return collections[name];
    }

    const Collection& getCollection(const string& name) const {  // const overload
        auto it = collections.find(name);
        if (it != collections.end()) {
            return it->second;
        }
        throw runtime_error("Collection not found: " + name);
    }

    set<string> listCollections() const {
        set<string> keys;
        for (const auto& pair : collections) {
            keys.insert(pair.first);
        }
        return keys;
    }

private:
    unordered_map<string, Collection> collections;
};

// === MongoLikeSystem: manages users ===
class System {
public:
    void createUser(const string& username) {
        if (users.find(username) == users.end()) {
            users[username] = UserDB();
        }
    }

    void createCollection(const string& username, const string& collectionName) {
        users[username].createCollection(collectionName);
    }

    void insertDocument(const string& username, const string& collectionName, Document doc) {
        users[username].getCollection(collectionName).insert(doc);
    }

    vector<Document> getDocuments(const string& username, const string& collectionName) const {
        return users.at(username).getCollection(collectionName).findAll();
    }

    int countDocuments(const string& username, const string& collectionName) const {
        return users.at(username).getCollection(collectionName).countDocuments();
    }

    int sumField(const string& username, const string& collectionName, const string& key) const {
        return users.at(username).getCollection(collectionName).sum(key);
    }

    set<string> distinctValues(const string& username, const string& collectionName, const string& key) const {
        return users.at(username).getCollection(collectionName).distinct(key);
    }

    set<string> listCollections(const string& username) const {
        return users.at(username).listCollections();
    }

private:
    unordered_map<string, UserDB> users;
};

// === Helpers for JSON conversion ===
string toJson(const Document& doc) {
    string json = "{";
    for (auto it = doc.begin(); it != doc.end(); ++it) {
        json += "\"" + it->first + "\": \"" + it->second + "\"";
        if (next(it) != doc.end()) json += ", ";
    }
    json += "}";
    return json;
}

string toJsonArray(const vector<Document>& docs) {
    string json = "[";
    for (size_t i = 0; i < docs.size(); ++i) {
        json += toJson(docs[i]);
        if (i < docs.size() - 1) json += ", ";
    }
    json += "]";
    return json;
}

// ------------------------- Simple HTTP Utilities -------------------------

// Split a string by a given delimiter
vector<string> split(const string& s, char delimiter) {
    vector<string> tokens;
    istringstream iss(s);
    string token;
    while (getline(iss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Parse a query string (e.g., "field=duration") into a map
unordered_map<string, string> parseQuery(const string &query) {
    unordered_map<string, string> params;
    auto pairs = split(query, '&');
    for (const auto &p : pairs) {
        auto kv = split(p, '=');
        if (kv.size() == 2) {
            params[kv[0]] = kv[1];
        }
    }
    return params;
}

// A very simple JSON parser for documents that expects a flat JSON object with string values.
// For example: {"key": "value", "priority": "1"}
Document parseJson(const string &json) {
    Document doc;
    size_t start = json.find("{");
    size_t end = json.find("}");
    if (start == string::npos || end == string::npos || start >= end) {
        return doc;
    }
    string inner = json.substr(start + 1, end - start - 1);
    auto pairs = split(inner, ',');
    for (auto &p : pairs) {
        auto kv = split(p, ':');
        if (kv.size() == 2) {
            // Remove quotes and spaces.
            auto trim = [](string s) {
                while (!s.empty() && isspace(s.back())) s.pop_back();
                while (!s.empty() && isspace(s.front())) s.erase(s.begin());
                if (!s.empty() && s.front() == '\"') s.erase(s.begin());
                if (!s.empty() && s.back() == '\"') s.pop_back();
                return s;
            };
            string key = trim(kv[0]);
            string value = trim(kv[1]);
            doc[key] = value;
        }
    }
    return doc;
}

// Send a HTTP response with given status code, content type, and body.
void sendHttpResponse(int clientSocket, int statusCode, const string &body, const string &contentType = "application/json") {
    string statusText = (statusCode == 200) ? "OK" : "Error";
    ostringstream oss;
    oss << "HTTP/1.1 " << statusCode << " " << statusText << "\r\n"
        << "Content-Type: " << contentType << "\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n"
        << "\r\n"
        << body;
    string response = oss.str();
    send(clientSocket, response.c_str(), response.size(), 0);
}

// ------------------------- HTTP Request Handler -------------------------

// Global database instance and a mutex to protect it (since our server uses threads).
System db;
mutex dbMutex;

void handleConnection(int clientSocket) {
    const int bufferSize = 4096;
    char buffer[bufferSize];
    memset(buffer, 0, bufferSize);
    
    int received = recv(clientSocket, buffer, bufferSize - 1, 0);
    if (received <= 0) {
        close(clientSocket);
        return;
    }
    string request(buffer, received);
    
    // Get the request line (first line)
    istringstream requestStream(request);
    string requestLine;
    getline(requestStream, requestLine);
    
    // Remove carriage return if exists
    if (!requestLine.empty() && requestLine.back() == '\r') {
        requestLine.pop_back();
    }
    
    // Parse method, URL, HTTP version
    istringstream lineStream(requestLine);
    string method, url, httpVersion;
    lineStream >> method >> url >> httpVersion;
    
    // Parse query string if present
    string path = url;
    string queryStr;
    auto pos = url.find("?");
    if (pos != string::npos) {
        path = url.substr(0, pos);
        queryStr = url.substr(pos + 1);
    }
    auto queryParams = parseQuery(queryStr);
    
    // Split path into segments
    auto segments = split(path, '/');  // note: first element may be empty because path starts with '/'
    
    string responseBody;
    int responseCode = 200;
    
    try {
        // Route the request
        if (method == "POST") {
            // Endpoint: POST /user/<username> => create user
            if (segments.size() >= 3 && segments[1] == "user" && segments.size() == 3) {
                string username = segments[2];
                {
                    lock_guard<mutex> lock(dbMutex);
                    db.createUser(username);
                }
                responseBody = "{\"status\": \"User created\"}";
            }
            // Endpoint: POST /user/<username>/collection/<collectionName> => create collection
            else if (segments.size() >= 5 && segments[1] == "user" && segments[3] == "collection" && segments.size() == 5) {
                string username = segments[2];
                string collectionName = segments[4];
                {
                    lock_guard<mutex> lock(dbMutex);
                    db.createUser(username);  // ensure user exists
                    db.createCollection(username, collectionName);
                }
                responseBody = "{\"status\": \"Collection created\"}";
            }
            // Endpoint: POST /user/<username>/collection/<collectionName>/document => insert document.
            else if (segments.size() >= 7 && segments[1] == "user" && segments[3] == "collection" && segments[5] == "document" && segments.size() == 7) {
                string username = segments[2];
                string collectionName = segments[4];
                // The rest of the request (after header) is the body
                string body;
                // Read until the end of headers (look for "\r\n\r\n")
                size_t headerEnd = request.find("\r\n\r\n");
                if (headerEnd != string::npos) {
                    body = request.substr(headerEnd + 4);
                }
                Document doc = parseJson(body);
                {
                    lock_guard<mutex> lock(dbMutex);
                    db.createUser(username);  // ensure user exists
                    db.createCollection(username, collectionName);
                    db.insertDocument(username, collectionName, doc);
                }
                responseBody = "{\"status\": \"Document inserted\"}";
            }
            else {
                responseCode = 404;
                responseBody = "{\"error\": \"Endpoint not found\"}";
            }
        } 
        else if (method == "GET") {
            // Endpoint: GET /user/<username>/collection/<collectionName>/documents => list documents
            if (segments.size() >= 6 && segments[1] == "user" && segments[3] == "collection" && segments[5] == "documents") {
                string username = segments[2];
                string collectionName = segments[4];
                vector<Document> docs;
                {
                    lock_guard<mutex> lock(dbMutex);
                    docs = db.getDocuments(username, collectionName);
                }
                responseBody = toJsonArray(docs);
            }
            // Endpoint: GET /user/<username>/collection/<collectionName>/count => count documents
            else if (segments.size() >= 6 && segments[1] == "user" && segments[3] == "collection" && segments[5] == "count") {
                string username = segments[2];
                string collectionName = segments[4];
                int count = 0;
                {
                    lock_guard<mutex> lock(dbMutex);
                    count = db.countDocuments(username, collectionName);
                }
                responseBody = "{\"count\": " + to_string(count) + "}";
            }
            // Endpoint: GET /user/<username>/collection/<collectionName>/sum?field=<key> => sum field
            else if (segments.size() >= 6 && segments[1] == "user" && segments[3] == "collection" && segments[5] == "sum") {
                string username = segments[2];
                string collectionName = segments[4];
                if (queryParams.find("field") == queryParams.end()) {
                    responseCode = 400;
                    responseBody = "{\"error\": \"Missing query parameter: field\"}";
                } else {
                    string field = queryParams["field"];
                    int sum = 0;
                    {
                        lock_guard<mutex> lock(dbMutex);
                        sum = db.sumField(username, collectionName, field);
                    }
                    responseBody = "{\"sum\": " + to_string(sum) + "}";
                }
            }
            // Endpoint: GET /user/<username>/collection/<collectionName>/distinct?field=<key> => distinct values
            else if (segments.size() >= 6 && segments[1] == "user" && segments[3] == "collection" && segments[5] == "distinct") {
                string username = segments[2];
                string collectionName = segments[4];
                if (queryParams.find("field") == queryParams.end()) {
                    responseCode = 400;
                    responseBody = "{\"error\": \"Missing query parameter: field\"}";
                } else {
                    string field = queryParams["field"];
                    set<string> distinctVals;
                    {
                        lock_guard<mutex> lock(dbMutex);
                        distinctVals = db.distinctValues(username, collectionName, field);
                    }
                    // Build JSON array from set
                    string jsonArray = "[";
                    bool first = true;
                    for (const auto &val : distinctVals) {
                        if (!first) jsonArray += ", ";
                        jsonArray += "\"" + val + "\"";
                        first = false;
                    }
                    jsonArray += "]";
                    responseBody = jsonArray;
                }
            }
            // Endpoint: GET /user/<username>/collections => list collections for user
            else if (segments.size() >= 4 && segments[1] == "user" && segments[3] == "collections") {
                string username = segments[2];
                set<string> collections;
                {
                    lock_guard<mutex> lock(dbMutex);
                    collections = db.listCollections(username);
                }
                // Build JSON array from set
                string jsonArray = "[";
                bool first = true;
                for (const auto &coll : collections) {
                    if (!first) jsonArray += ", ";
                    jsonArray += "\"" + coll + "\"";
                    first = false;
                }
                jsonArray += "]";
                responseBody = jsonArray;
            }
            else {
                responseCode = 404;
                responseBody = "{\"error\": \"Endpoint not found\"}";
            }
        }
        else {
            responseCode = 405;
            responseBody = "{\"error\": \"Method not allowed\"}";
        }
    }
    catch (exception &ex) {
        responseCode = 500;
        responseBody = string("{\"error\": \"") + ex.what() + "\"}";
    }
    
    sendHttpResponse(clientSocket, responseCode, responseBody);
    close(clientSocket);
}

// ------------------------- Main: Server Loop -------------------------

int main() {
    // Create a socket (IPv4, TCP)
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        cerr << "Error creating socket\n";
        return 1;
    }

    // Allow quick reuse of the port
    int opt = 1;
    setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to port 8080 on all interfaces
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(8080);
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    memset(&(serverAddr.sin_zero), '\0', 8);

    if (bind(serverSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr)) == -1) {
        cerr << "Error binding to port\n";
        close(serverSocket);
        return 1;
    }

    if (listen(serverSocket, 10) == -1) {
        cerr << "Error listening on socket\n";
        close(serverSocket);
        return 1;
    }

    cout << "Server running on port 8080...\n";

    while (true) {
        sockaddr_in clientAddr;
        socklen_t clientSize = sizeof(clientAddr);
        int clientSocket = accept(serverSocket, (struct sockaddr *)&clientAddr, &clientSize);
        if (clientSocket == -1) {
            cerr << "Error accepting connection\n";
            continue;
        }

        // Handle each connection in a separate thread.
        thread t(handleConnection, clientSocket);
        t.detach();
    }

    close(serverSocket);
    return 0;
}
