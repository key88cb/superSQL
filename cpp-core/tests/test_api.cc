#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include "../api.h"
#include "../basic.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

BufferManager buffer_manager; // Extern

void setup_env() {
    std::string cat_path = "./database/catalog/catalog_file";
    std::ofstream ofs(cat_path, std::ios::binary);
    char buf[PAGESIZE];
    std::memset(buf, 0, PAGESIZE);
    ofs.write(buf, PAGESIZE);
    ofs.close();
}

void test_api_full_flow() {
    std::cout << "Running test_api_full_flow..." << std::endl;
    API api;
    
    std::string table_name = "api_test_table";
    Attribute attr;
    attr.num = 3;
    attr.name[0] = "id"; attr.type[0] = -1; attr.unique[0] = true;
    attr.name[1] = "name"; attr.type[1] = 10; attr.unique[1] = false;
    attr.name[2] = "score"; attr.type[2] = 0; attr.unique[2] = false;
    attr.primary_key = 0;
    
    Index idx; idx.num = 0;

    // 1. Create Table
    ASSERT_TRUE(api.createTable(table_name, attr, 0, idx));

    // 2. Insert Records
    Tuple t1;
    Data d1a; d1a.type = -1; d1a.datai = 1;
    Data d1b; d1b.type = 10; d1b.datas = "alice";
    Data d1c; d1c.type = 0; d1c.dataf = 95.5f;
    t1.addData(d1a); t1.addData(d1b); t1.addData(d1c);
    api.insertRecord(table_name, t1);

    Tuple t2;
    Data d2a; d2a.type = -1; d2a.datai = 2;
    Data d2b; d2b.type = 10; d2b.datas = "bob";
    Data d2c; d2c.type = 0; d2c.dataf = 88.0f;
    t2.addData(d2a); t2.addData(d2b); t2.addData(d2c);
    api.insertRecord(table_name, t2);

    // 3. Select with condition (id = 1)
    std::vector<std::string> cond_attrs = {"id"};
    std::vector<Where> wheres;
    Where w1;
    w1.data.type = -1; w1.data.datai = 1;
    w1.relation_character = EQUAL;
    wheres.push_back(w1);

    Table res = api.selectRecord(table_name, cond_attrs, wheres, 'a'); // 'a' for AND
    ASSERT_EQ(res.getTuple().size(), 1);
    ASSERT_EQ(res.getTuple()[0].getData()[1].datas, "alice");

    // 4. Create Index
    ASSERT_TRUE(api.createIndex(table_name, "name_idx", "name"));

    // 5. Select with condition after index (name = 'bob')
    std::vector<std::string> cond_attrs2 = {"name"};
    std::vector<Where> wheres2;
    Where w2;
    w2.data.type = 10; w2.data.datas = "bob";
    w2.relation_character = EQUAL;
    wheres2.push_back(w2);
    
    Table res2 = api.selectRecord(table_name, cond_attrs2, wheres2, 'a');
    ASSERT_EQ(res2.getTuple().size(), 1);
    ASSERT_EQ(res2.getTuple()[0].getData()[1].datas, "bob");

    // 6. Delete Record
    api.deleteRecord(table_name, "id", w1);
    Table res3 = api.selectRecord(table_name, {}, {}, 'a');
    ASSERT_EQ(res3.getTuple().size(), 1);

    // 7. Drop Index
    ASSERT_TRUE(api.dropIndex(table_name, "name_idx"));

    // 8. Drop Table
    ASSERT_TRUE(api.dropTable(table_name));
}

int main() {
    setup_env();
    test_api_full_flow();
    TEST_REPORT();
}
