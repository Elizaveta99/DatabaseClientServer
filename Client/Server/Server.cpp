#pragma comment(lib, "ws2_32.lib")
#pragma warning(disable: 4996)

#include "custom_database.pb.h"
#include <winsock2.h>
#include <Ws2tcpip.h>
#include <iostream>
#include <google/protobuf/text_format.h>
#include <process.h>
#include "Database.h"

using json = nlohmann::json;

void sendAnswertoClient(json row, SOCKET &newConnection) {
	std::string ansMessage = row["message"].get<std::string>();

	Answer answer;
	answer.set_answer(ansMessage);
	std::string ansToSend;

	int size1 = answer.ByteSize();
	unsigned NetInt = htonl((unsigned)size1);
	send(newConnection, (char*)&NetInt, sizeof(long), 0);

	answer.SerializeToString(&ansToSend);
	send(newConnection, ansToSend.c_str(), size1, NULL);
}

unsigned int __stdcall  workWithDB(void* data);

int main(int argc, char* argv[]) {

	//WSAStartup
	WSAData wsaData;
	WORD DLLVersion = MAKEWORD(2, 2);
	if (WSAStartup(DLLVersion, &wsaData) != 0) {
		std::cout << "Error" << std::endl;
		exit(1);
	}

	sockaddr_in addr;
	int sizeofaddr = sizeof(addr);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	addr.sin_port = htons(5555);
	addr.sin_family = AF_INET;

	SOCKET sListen = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sListen == INVALID_SOCKET) {
		std::cout << "socket error + " << WSAGetLastError() << "\n";
		exit(1);
	}
	int bnd =  bind(sListen, (SOCKADDR*)&addr, sizeofaddr);
	if (bnd == SOCKET_ERROR) {
		std::cout << "bind error + " << WSAGetLastError() << "\n";
		exit(1);
	}
	int lt = listen(sListen, SOMAXCONN);
	if (lt != 0) {
		std::cout << "listen error + " << WSAGetLastError() << "\n";
		exit(1);
	}

	SOCKET newConnection;

	while (newConnection = accept(sListen, (SOCKADDR*)&addr, &sizeofaddr)) {

		if (newConnection == INVALID_SOCKET) {
			std::cout << "connection error + " << WSAGetLastError() << "\n";
			exit(1);
		}

		_beginthreadex(0, 0, workWithDB, (void*)&newConnection, 0, 0);
	}
	
	return 0;
}

unsigned int __stdcall workWithDB(void* data)
{
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	SOCKET* client = (SOCKET*)data;
	SOCKET Client = *client;
	std::cout << "Client Connected!\n";

	DatabaseLib::Database database;
	DatabaseLib::Connection connection = database.connect();

	for (;;) {

		int type;
		unsigned NetInt;
		recv(Client, (char*)&NetInt, sizeof(long), 0);
		type = (int)ntohl(NetInt);

		if (type == 0) {
			std::cout << "Exit server!\n";
			break;
		}

		int size;
		recv(Client, (char*)&NetInt, sizeof(long), 0);
		size = (int)ntohl(NetInt);

		std::string message;
		char* msg = new char[size];
		recv(Client, msg, size, NULL);
		message = msg;

		switch (type)
		{
			case (1):
			{
				CreateTableRequest createTableRequest;
				createTableRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(createTableRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				json keys;
				for (int i = 0; i < createTableRequest.keys().size(); i++) {
					CompositeKey compositeKey = createTableRequest.keys(i);
					keys[compositeKey.c_key_name()] = {};
					for (int j = 0; j < compositeKey.c_keys().size(); j++) {
						keys[compositeKey.c_key_name()] += compositeKey.c_keys(j);
					}
				}
				json row;
				try
				{
					database.createTable(createTableRequest.table_name(), keys, connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}
				row["message"] = "Table created successfully!";
				sendAnswertoClient(row, Client);
				break;
			}
			case (2):
			{
				SimpleTableRequest deleteTableRequest;
				deleteTableRequest.ParseFromString(message);
				json row;
				try
				{
					database.removeTable(deleteTableRequest.table_name(), connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}
				row["message"] = "Table deleted successfully!";
				sendAnswertoClient(row, Client);
				break;
			}
			case (3):
			{
				GetRowByKeyRequest getRowByKeyRequest;
				getRowByKeyRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(getRowByKeyRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				json key;
				CompositeKeyValue compositeKeyValue = getRowByKeyRequest.key_value(0);
				key[compositeKeyValue.c_key_name()] = {};
				for (int j = 0; j < compositeKeyValue.c_keys_values().size(); j++) {
					json keyValue;
					keyValue[compositeKeyValue.c_keys(j)] = compositeKeyValue.c_keys_values(j);
					key[compositeKeyValue.c_key_name()] += keyValue;
				}
				std::cout << key << "\n";

				json row;
				try
				{
					row = database.getRowByKey(getRowByKeyRequest.table_name(), key, connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}

				sendAnswertoClient(row, Client);
				break;
			}
			case(4):
			{
				GetRowInSortedTableRequest getRowInSortedTableRequest;
				getRowInSortedTableRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(getRowInSortedTableRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				bool isReversed = false;
				if (getRowInSortedTableRequest.is_reversed() == "true") {
					isReversed = true;
				}

				json row;
				try
				{
					row = database.getRowInSortedTable(getRowInSortedTableRequest.table_name(), getRowInSortedTableRequest.key_name(), isReversed, connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}

				sendAnswertoClient(row, Client);
				break;
			}
			case (5):
			{
				SimpleTableRequest getNextRowRequest;
				getNextRowRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(getNextRowRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				json row;
				try
				{
					row = database.getNextRow(getNextRowRequest.table_name(), connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}

				sendAnswertoClient(row, Client);
				break;
			}
			case (6):
			{
				SimpleTableRequest getPrevRowRequest;
				getPrevRowRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(getPrevRowRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				json row;
				try
				{
					row = database.getPrevRow(getPrevRowRequest.table_name(), connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}

				sendAnswertoClient(row, Client);
				break;
			}
			case (7):
			{
				AppendRowRequest appendRowRequest;
				appendRowRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(appendRowRequest, &ans);
				std::cout << "ans " << ans << std::endl;
				json keys;
				for (int i = 0; i < appendRowRequest.keys_values().size(); i++) {
					CompositeKeyValue compositeKeyValue = appendRowRequest.keys_values(i);
					keys[compositeKeyValue.c_key_name()] = {};
					for (int j = 0; j < compositeKeyValue.c_keys_values().size(); j++) {
						json keyValue;
						keyValue[compositeKeyValue.c_keys(j)] = compositeKeyValue.c_keys_values(j);
						keys[compositeKeyValue.c_key_name()] += keyValue;
					}
				}

				json value;
				std::string key_value = "message";
				value[key_value] = appendRowRequest.value();

				std::cout << keys << "\n" << value << "\n";

				json row;
				try 
				{
					database.appendRow(appendRowRequest.table_name(), keys, value, connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}
				row["message"] = "Row inserted successfully!";
				sendAnswertoClient(row, Client);
				break;
			}
			case (8):
			{
				SimpleTableRequest removeRowRequest;
				removeRowRequest.ParseFromString(message);
				json row;
				try
				{
					database.removeRow(removeRowRequest.table_name(), connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}
				row["message"] = "Row deleted successfully!";
				sendAnswertoClient(row, Client);
				break;
			}
			case (9):
			{
				AddKeyRequest addKeyRequest;
				addKeyRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(addKeyRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				json key;
				CompositeKey compositeKey = addKeyRequest.key(0);
				key[compositeKey.c_key_name()] = {};
				for (int j = 0; j < compositeKey.c_keys().size(); j++) {
					key[compositeKey.c_key_name()] += compositeKey.c_keys(j);
				}

				json row;
				try
				{
					database.addKey(addKeyRequest.table_name(), key, connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}
				row["message"] = "Key inserted successfully!";
				sendAnswertoClient(row, Client);
				break;
			}
			case (10):
			{
				RemoveKeyRequest removeKeyRequest;
				removeKeyRequest.ParseFromString(message);

				std::string ans;
				google::protobuf::TextFormat::PrintToString(removeKeyRequest, &ans);
				std::cout << "ans " << ans << std::endl;

				json row;
				try
				{
					database.removeKey(removeKeyRequest.table_name(), removeKeyRequest.key_name(), connection);
				}
				catch (DatabaseLib::DatabaseException ex)
				{
					row["message"] = ex.what();
					sendAnswertoClient(row, Client);
					break;
				}
				row["message"] = "Key deleted successfully!";
				sendAnswertoClient(row, Client);
				break;
			}
		}
	}

	database.disconnect(connection);
	google::protobuf::ShutdownProtobufLibrary();
	system("pause");
	return 0;
}