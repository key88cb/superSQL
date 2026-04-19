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

BufferManager* buffer_manager_ptr = new BufferManager(); // Initialize the global pointer

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

    // 6. Select with 3 conditions (AND)
    std::vector<std::string> cond_attrs3 = {"id", "score", "name"};
    std::vector<Where> wheres3;
    Where w3_id;
    w3_id.data.type = -1; w3_id.data.datai = 0;
    w3_id.relation_character = GREATER;
    wheres3.push_back(w3_id);
    Where w3_score;
    w3_score.data.type = 0; w3_score.data.dataf = 90.0f;
    w3_score.relation_character = GREATER;
    wheres3.push_back(w3_score);
    Where w3_name;
    w3_name.data.type = 10; w3_name.data.datas = "alice";
    w3_name.relation_character = EQUAL;
    wheres3.push_back(w3_name);

    Table res3 = api.selectRecord(table_name, cond_attrs3, wheres3, 'a');
    ASSERT_EQ(res3.getTuple().size(), 1);
    ASSERT_EQ(res3.getTuple()[0].getData()[1].datas, "alice");

    // 7. Select with 3 conditions (OR)
    std::vector<std::string> cond_attrs4 = {"id", "score", "name"};
    std::vector<Where> wheres4;
    Where w4_id;
    w4_id.data.type = -1; w4_id.data.datai = 2;
    w4_id.relation_character = EQUAL;
    wheres4.push_back(w4_id);
    Where w4_score;
    w4_score.data.type = 0; w4_score.data.dataf = 95.0f;
    w4_score.relation_character = GREATER;
    wheres4.push_back(w4_score);
    Where w4_name;
    w4_name.data.type = 10; w4_name.data.datas = "nobody";
    w4_name.relation_character = EQUAL;
    wheres4.push_back(w4_name);

    Table res4 = api.selectRecord(table_name, cond_attrs4, wheres4, 0);
    ASSERT_EQ(res4.getTuple().size(), 2);

    // 8. Delete Record
    api.deleteRecord(table_name, "id", w1);
    Table res5 = api.selectRecord(table_name, {}, {}, 'a');
    ASSERT_EQ(res5.getTuple().size(), 1);

    // 9. Drop Index
    ASSERT_TRUE(api.dropIndex(table_name, "name_idx"));

    // 10. Drop Table
    ASSERT_TRUE(api.dropTable(table_name));
}

int main() {
    setup_env();
    test_api_full_flow();
    TEST_REPORT();
}
