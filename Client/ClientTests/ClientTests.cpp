#include "pch.h"
#include "CppUnitTest.h"
#include "Client.cpp"

using namespace Microsoft::VisualStudio::CppUnitTestFramework;

namespace ClientTests
{
	TEST_CLASS(ClientTests)
	{
	private:
		SOCKET Connection;

	public:
		CreateTableRequest createTableHelp() {
			CreateTableRequest createTableRequest;
			createTableRequest.set_table_name("clients");
			createTableRequest.set_amount_keys(2);
			CompositeKey* compositeKey;
			compositeKey = createTableRequest.add_keys();
			compositeKey->set_c_key_name("idNameKey");
			compositeKey->set_c_amount(2);
			compositeKey->add_c_keys("id");
			compositeKey->add_c_keys("name");

			compositeKey = createTableRequest.add_keys();
			compositeKey->set_c_key_name("emailKey");
			compositeKey->set_c_amount(1);
			compositeKey->add_c_keys("email");

			return createTableRequest;
		}

		SimpleTableRequest deleteTableHelp() {
			SimpleTableRequest deleteTableRequest;
			deleteTableRequest.set_table_name("clients");
			return deleteTableRequest;
		}

		TEST_METHOD_INITIALIZE(createConnectionMethod)
		{
			Connection = createConnection();
		}
		
		TEST_METHOD(createTableMethod)
		{
			convertTosend(1, Connection);
			CreateTableRequest createTableRequest = createTableHelp();

			std::string expected = "answer: \"Table created successfully!\"\n";
			Assert::AreEqual(createTable(createTableRequest, Connection), expected);

			tableNameAction(deleteTableHelp(), Connection);
		}

		TEST_METHOD(deleteTableMethod)
		{
			convertTosend(2, Connection);
			CreateTableRequest createTableRequest = createTableHelp();

			std::string expected = "answer: \"Table deleted successfully!\"\n";
			Assert::AreEqual(tableNameAction(deleteTableHelp(), Connection), expected);
		}

		TEST_METHOD(getRowByKeyMethod)
		{
			convertTosend(3, Connection);
			CreateTableRequest createTableRequest = createTableHelp();

			SimpleTableRequest deleteTableRequest;
			deleteTableRequest.set_table_name("clients");

			std::string expected = "answer: \"Table deleted successfully!\"\n";
			Assert::AreEqual(tableNameAction(deleteTableRequest, Connection), expected);

			tableNameAction(deleteTableHelp(), Connection);
		}

		TEST_METHOD_CLEANUP(deleteConnectionMethod)
		{
			closesocket(Connection);
		}
	};
}
