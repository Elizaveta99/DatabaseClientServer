#pragma comment(lib, "ws2_32.lib")
#pragma warning(disable: 4996)

#include <winsock2.h>
#include <iostream>
#include <vector>
#include "custom_database.pb.h"
#include <google/protobuf/text_format.h>

using namespace std;

void addCompositeKey(CompositeKey* compositeKey) {
	string key_name;
	cin >> key_name;
	compositeKey->set_c_key_name(key_name);
	int amount;
	cin >> amount;
	compositeKey->set_c_amount(amount);
	for (int i = 0; i < amount; i++) {
		string key;
		cin >> key;
		compositeKey->add_c_keys(key);
	}
}

void addCompositeKeyValue(CompositeKeyValue* compositeKeyValue) {
	string key_name;
	cin >> key_name;
	compositeKeyValue->set_c_key_name(key_name);
	int amount;
	cin >> amount;
	compositeKeyValue->set_c_amount(amount);
	for (int i = 0; i < amount; i++) {
		string key;
		cin >> key;
		compositeKeyValue->add_c_keys(key);
	}
	for (int i = 0; i < amount; i++) {
		string key_value;
		cin >> key_value;
		compositeKeyValue->add_c_keys_values(key_value);
	}
}

string recvAnswerFromServer(SOCKET &Connection) {
	int size;
	unsigned NetInt;
	recv(Connection, (char*)&NetInt, sizeof(long), 0);
	size = (int)ntohl(NetInt);

	std::string ansMessage;
	char* msg = new char[size];
	recv(Connection, msg, size, NULL);
	ansMessage = msg;

	Answer answer;
	answer.ParseFromString(ansMessage);

	string ansToPrint;
	google::protobuf::TextFormat::PrintToString(answer, &ansToPrint);
	return ansToPrint;
	//cout << ansToPrint << "\n";
}

void convertTosend(long size, SOCKET &Connection) {
	unsigned NetInt = htonl((unsigned)size);
	send(Connection, (char*)&NetInt, sizeof(long), 0);
}

string createTable(CreateTableRequest createTableRequest, SOCKET &Connection) {
	long size = createTableRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	createTableRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string tableNameAction(SimpleTableRequest simpleTableRequest, SOCKET &Connection) {
	long size = simpleTableRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	simpleTableRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string getRowByKey(GetRowByKeyRequest getRowByKeyRequest, SOCKET &Connection) {
	long size = getRowByKeyRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	getRowByKeyRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string getRowInSortedTable(GetRowInSortedTableRequest getRowInSortedTableRequest, SOCKET &Connection) {
	long size = getRowInSortedTableRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	getRowInSortedTableRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string getNextRow(GetRowInSortedTableRequest getRowInSortedTableRequest, SOCKET &Connection) {
	long size = getRowInSortedTableRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	getRowInSortedTableRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string appendRow(AppendRowRequest appendRowRequest, SOCKET &Connection) {
	long size = appendRowRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	appendRowRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string addKey(AddKeyRequest addKeyRequest, SOCKET &Connection) {
	long size = addKeyRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	addKeyRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

string removeKey(RemoveKeyRequest removeKeyRequest, SOCKET &Connection) {
	long size = removeKeyRequest.ByteSize();
	convertTosend(size, Connection);

	string message;
	removeKeyRequest.SerializeToString(&message);
	send(Connection, message.c_str(), size, NULL);

	return recvAnswerFromServer(Connection);
}

SOCKET createConnection() {
	//WSAStartup
	WSAData wsaData;
	WORD DLLVersion = MAKEWORD(2, 2);
	if (WSAStartup(DLLVersion, &wsaData) != 0) {
		cout << "Error" << std::endl;
		return 1;
	}

	SOCKADDR_IN addr;
	int sizeofaddr = sizeof(addr);
	addr.sin_addr.s_addr = inet_addr("127.0.0.1");
	addr.sin_port = htons(5555);
	addr.sin_family = AF_INET;

	SOCKET Connection = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (Connection == INVALID_SOCKET) {
		cout << "socket + " << WSAGetLastError() << endl;
		return 1;
	}
	if (connect(Connection, (SOCKADDR*)&addr, sizeof(addr)) != 0) {
		cout << "Error: failed connect to server.\n";
		return 1;
	}
	cout << "Connected to server!\n";

	return Connection;
}

int main(int argc, char* argv[]) {
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	cout << "\nMenu\n";
	cout << "-------------------------------------------------------------------------\n";
	cout << "Create table -\n\t<1 table_name amount_keys key_name_1 amount_1 k11 k12 k1n key_name_2 amount_2 k21 k22 k2m>\n";
	cout << "Delete table -\n\t<2 table_name>\n";
	cout << "Get row by key -\n\t<3 table_name key_name amount k1 k2 kn k1_value k2_value kn_value>\n";
	cout << "Get first/last row by key -\n\t<4 table_name key_name false/true>\n";
	cout << "Get next row -\n\t<5 table_name>\n";
	cout << "Get prev row -\n\t<6 table_name>\n";
	cout << "Append row -\n\t<7 table_name amount_keys key_name_1 amount_1 k11 k12 k1n k11_value k12_value k1n_value key_name_2 amount_2 k21 k22 k2m k21_value k22_value k2m_value value>\n";
	cout << "Remove row -\n\t<8 table_name>\n";
	cout << "Add key -\n\t<9 table_name key_name amount k1 k2 kn>\n";
	cout << "Remove key -\n\t<10 table_name key_name>\n";
	cout << "Exit -\n\t<0>\n";

	SOCKET Connection = createConnection();
	if (Connection == 1) {
		return 0;
	}

	for (;;) {
		string table_name;
		int type;
		cin >> type;
		convertTosend(type, Connection);

		if (type == 0) {
			cout << "Exit client!\n";
			break;
		}

		switch (type)
		{
			case (1):
			{
				CreateTableRequest createTableRequest;
				cin >> table_name;
				createTableRequest.set_table_name(table_name);
				int amount_keys;
				cin >> amount_keys;
				createTableRequest.set_amount_keys(amount_keys);
				for (int i = 0; i < amount_keys; i++) {
					addCompositeKey(createTableRequest.add_keys());
				}
				cout << createTable(createTableRequest, Connection) << "\n";
				break;
			}
			case (2):
			{
				SimpleTableRequest deleteTableRequest;
				cin >> table_name;
				deleteTableRequest.set_table_name(table_name);
				cout << tableNameAction(deleteTableRequest, Connection) << "\n";
				break;
			}
			case (3):
			{
				GetRowByKeyRequest getRowByKeyRequest;
				cin >> table_name;
				getRowByKeyRequest.set_table_name(table_name);
				addCompositeKeyValue(getRowByKeyRequest.add_key_value());
				cout << getRowByKey(getRowByKeyRequest, Connection) << "\n";
				break;
			}
			case (4):
			{
				GetRowInSortedTableRequest getRowInSortedTableRequest;
				cin >> table_name;
				getRowInSortedTableRequest.set_table_name(table_name);
				string key_name;
				cin >> key_name;
				getRowInSortedTableRequest.set_key_name(key_name);
				string is_reversed_str;
				cin >> is_reversed_str;
				getRowInSortedTableRequest.set_is_reversed(is_reversed_str);
				cout << getRowInSortedTable(getRowInSortedTableRequest, Connection) << "\n";
				break;
			}
			case (5):
			{
				SimpleTableRequest getNextRowRequest;
				cin >> table_name;
				getNextRowRequest.set_table_name(table_name);
				cout << tableNameAction(getNextRowRequest, Connection) << "\n";
				break;
			}
			case (6):
			{
				SimpleTableRequest getPrevRowRequest;
				cin >> table_name;
				getPrevRowRequest.set_table_name(table_name);
				cout << tableNameAction(getPrevRowRequest, Connection) << "\n";
				break;
			}
			case (7):
			{
				AppendRowRequest appendRowRequest;
				cin >> table_name;
				appendRowRequest.set_table_name(table_name);
				int amount_keys;
				cin >> amount_keys;
				appendRowRequest.set_amount_keys(amount_keys);
				for (int i = 0; i < amount_keys; i++) {
					addCompositeKeyValue(appendRowRequest.add_keys_values());
				}
				string value;
				getline(cin, value);
				appendRowRequest.set_value(value);
				cout << appendRow(appendRowRequest, Connection) << "\n";
				break;
			}
			case (8):
			{
				SimpleTableRequest removeRowRequest;
				cin >> table_name;
				removeRowRequest.set_table_name(table_name);
				cout << tableNameAction(removeRowRequest, Connection) << "\n";
				break;
			}
			case (9):
			{
				AddKeyRequest addKeyRequest;
				cin >> table_name;
				addKeyRequest.set_table_name(table_name);
				addCompositeKey(addKeyRequest.add_key());
				cout << addKey(addKeyRequest, Connection) << "\n";
				break;
			}
			case (10):
			{
				RemoveKeyRequest removeKeyRequest;
				cin >> table_name;
				removeKeyRequest.set_table_name(table_name);
				string key_name;
				cin >> key_name;
				removeKeyRequest.set_key_name(key_name);
				cout << removeKey(removeKeyRequest, Connection) << "\n";
				break;
			}
		}
	}

	closesocket(Connection);
	google::protobuf::ShutdownProtobufLibrary();
	return 0;
}



