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

#include "json.hpp" // <-- Download json.hpp and place in your directory

using json = nlohmann::json;
using namespace std;

using Document = unordered_map<string, string>;

// === Collection ===
class Collection {
public:
    void insert(Document doc) {
        doc["_id"] = to_string(nextId++);
        documents.push_back(doc);
    }

    vector<Document> findAll() const { return documents; }

    int countDocuments() const { return documents.size(); }

    int sum(const string& key) const {
        int total = 0;
        for (const auto& doc : documents)
            if (doc.count(key)) total += atoi(doc.at(key).c_str());
        return total;
    }

    set<string> distinct(const string& key) const {
        set<string> values;
        for (const auto& doc : documents)
            if (doc.count(key)) values.insert(doc.at(key));
        return values;
    }

private:
    vector<Document> documents;
    int nextId = 1;
};

class UserDB {
public:
    void createCollection(const string& name) {
        if (!collections.count(name))
            collections[name] = Collection();
    }

    Collection& getCollection(const string& name) {
        return collections.at(name);
    }

    const Collection& getCollection(const string& name) const {
        return collections.at(name);
    }

    set<string> listCollections() const {
        set<string> keys;
        for (const auto& [name, _] : collections) keys.insert(name);
        return keys;
    }

private:
    unordered_map<string, Collection> collections;
};

class System {
public:
    void createUser(const string& user) {
        if (!users.count(user)) users[user] = UserDB();
    }

    void createCollection(const string& user, const string& col) {
        users[user].createCollection(col);
    }

    void insertDocument(const string& user, const string& col, const Document& doc) {
        users[user].getCollection(col).insert(doc);
    }

    vector<Document> getDocuments(const string& user, const string& col) const {
        return users.at(user).getCollection(col).findAll();
    }

    int countDocuments(const string& user, const string& col) const {
        return users.at(user).getCollection(col).countDocuments();
    }

    int sumField(const string& user, const string& col, const string& key) const {
        return users.at(user).getCollection(col).sum(key);
    }

    set<string> distinctValues(const string& user, const string& col, const string& key) const {
        return users.at(user).getCollection(col).distinct(key);
    }

    set<string> listCollections(const string& user) const {
        return users.at(user).listCollections();
    }

private:
    unordered_map<string, UserDB> users;
};

// JSON utils
string toJson(const Document& doc) {
    json j(doc);
    return j.dump();
}

string toJsonArray(const vector<Document>& docs) {
    json j = json::array();
    for (auto& doc : docs) j.push_back(doc);
    return j.dump();
}

// HTTP helpers
unordered_map<string, string> parseQuery(const string& query) {
    unordered_map<string, string> params;
    istringstream ss(query);
    string pair;
    while (getline(ss, pair, '&')) {
        size_t eq = pair.find('=');
        if (eq != string::npos)
            params[pair.substr(0, eq)] = pair.substr(eq + 1);
    }
    return params;
}

vector<string> split(const string& s, char delim) {
    vector<string> tokens;
    istringstream ss(s);
    string item;
    while (getline(ss, item, delim)) tokens.push_back(item);
    return tokens;
}

Document parseJson(const string& body) {
    Document doc;
    auto j = json::parse(body);
    for (auto it = j.begin(); it != j.end(); ++it) {
        doc[it.key()] = it.value();
    }
    return doc;
}

void sendHttpResponse(int clientSocket, int statusCode, const string& body) {
    string statusText = (statusCode == 200) ? "OK" : "Error";
    ostringstream oss;
    oss << "HTTP/1.1 " << statusCode << " " << statusText << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n\r\n" << body;
    string response = oss.str();
    send(clientSocket, response.c_str(), response.size(), 0);
}

// Shared DB
System db;
mutex dbMutex;

// Main HTTP request handler
void handleConnection(int clientSocket) {
    const int BUF_SIZE = 8192;
    char buffer[BUF_SIZE] = {};
    int received = recv(clientSocket, buffer, BUF_SIZE - 1, 0);
    if (received <= 0) {
        close(clientSocket);
        return;
    }
    

    string request(buffer);
    istringstream ss(request);
    string method, url, version;
    ss >> method >> url >> version;

    string path = url;
    string query;
    if (auto q = url.find('?'); q != string::npos) {
        path = url.substr(0, q);
        query = url.substr(q + 1);
    }

    auto segments = split(path, '/');
    auto queryParams = parseQuery(query);

    string response;
    int code = 200;

    try {
        if (method == "POST") {
            if (segments.size() == 3 && segments[1] == "user") {
                string user = segments[2];
                lock_guard<mutex> lock(dbMutex);
                db.createUser(user);
                response = R"({"status": "User created"})";
            }
            else if (segments.size() == 5 && segments[1] == "user" && segments[3] == "collection") {
                string user = segments[2], col = segments[4];
                lock_guard<mutex> lock(dbMutex);
                db.createUser(user);
                db.createCollection(user, col);
                response = R"({"status": "Collection created"})";
            }
            else if (segments.size() == 6 && segments[5] == "document") {
                string user = segments[2], col = segments[4];

                string body = request.substr(request.find("\r\n\r\n") + 4);
                size_t contentLength = 0;
                if (auto pos = request.find("Content-Length:"); pos != string::npos) {
                    contentLength = stoi(request.substr(pos + 15));
                    while (body.size() < contentLength) {
                        char more[4096];
                        int got = recv(clientSocket, more, sizeof(more), 0);
                        if (got <= 0) break;
                        body.append(more, got);
                    }
                }

                Document doc = parseJson(body);
                lock_guard<mutex> lock(dbMutex);
                db.createUser(user);
                db.createCollection(user, col);
                db.insertDocument(user, col, doc);
                response = R"({"status": "Document inserted"})";
            } else {
                code = 404;
                response = R"({"error": "Unknown endpoint"})";
            }
        } else if (method == "GET") {
            if (segments.size() == 6 && segments[5] == "documents") {
                string user = segments[2], col = segments[4];
                lock_guard<mutex> lock(dbMutex);
                response = toJsonArray(db.getDocuments(user, col));
            }
            else if (segments.size() == 6 && segments[5] == "count") {
                string user = segments[2], col = segments[4];
                lock_guard<mutex> lock(dbMutex);
                response = "{\"count\": " + to_string(db.countDocuments(user, col)) + "}";
            }
            else if (segments.size() == 6 && segments[5] == "sum") {
                string user = segments[2], col = segments[4];
                string field = queryParams["field"];
                lock_guard<mutex> lock(dbMutex);
                response = "{\"sum\": " + to_string(db.sumField(user, col, field)) + "}";
            }
            else if (segments.size() == 6 && segments[5] == "distinct") {
                string user = segments[2], col = segments[4];
                string field = queryParams["field"];
                lock_guard<mutex> lock(dbMutex);
                json j = json::array();
                for (auto& val : db.distinctValues(user, col, field)) j.push_back(val);
                response = j.dump();
            }
            else if (segments.size() == 4 && segments[3] == "collections") {
                string user = segments[2];
                lock_guard<mutex> lock(dbMutex);
                json j = json::array();
                for (auto& val : db.listCollections(user)) j.push_back(val);
                response = j.dump();
            }
            else {
                code = 404;
                response = R"({"error": "Unknown endpoint"})";
            }
        } else {
            code = 405;
            response = R"({"error": "Method not allowed"})";
        }
    } catch (exception& e) {
        code = 500;
        response = "{\"error\": \"" + string(e.what()) + "\"}";
    }

    sendHttpResponse(clientSocket, code, response);
    close(clientSocket);
}

int main() {
    int server = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(server, (sockaddr*)&addr, sizeof(addr));
    listen(server, 10);

    cout << "Server running on http://localhost:8080\n";

    while (true) {
        sockaddr_in client;
        socklen_t size = sizeof(client);
        int clientSocket = accept(server, (sockaddr*)&client, &size);
        thread(handleConnection, clientSocket).detach();
    }

    close(server);
}
