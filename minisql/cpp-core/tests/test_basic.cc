#include <iostream>
#include <vector>
#include <string>
#include "../basic.h"
#include "test_macros.h"

int total_tests = 0;
int passed_tests = 0;

void test_data_structure() {
    std::cout << "Running test_data_structure..." << std::endl;
    Data d;
    d.type = -1; // int
    d.datai = 42;
    ASSERT_EQ(d.type, -1);
    ASSERT_EQ(d.datai, 42);

    d.type = 0; // float
    d.dataf = 3.14f;
    ASSERT_EQ(d.type, 0);
    // Be careful with float comparisons, but for direct assignment it's usually okay.
    ASSERT_TRUE(d.dataf > 3.13f && d.dataf < 3.15f);

    d.type = 10; // string
    d.datas = "hello";
    ASSERT_EQ(d.type, 10);
    ASSERT_EQ(d.datas, std::string("hello"));
}

void test_tuple() {
    std::cout << "Running test_tuple..." << std::endl;
    Tuple t;
    ASSERT_EQ(t.getSize(), 0);
    ASSERT_FALSE(t.isDeleted());

    Data d1; d1.type = -1; d1.datai = 10;
    Data d2; d2.type = 0; d2.dataf = 20.0f;
    
    t.addData(d1);
    ASSERT_EQ(t.getSize(), 1);
    
    t.addData(d2);
    ASSERT_EQ(t.getSize(), 2);

    std::vector<Data> data_vec = t.getData();
    ASSERT_EQ(data_vec[0].datai, 10);
    ASSERT_EQ(data_vec[1].dataf, 20.0f);

    t.setDeleted();
    ASSERT_TRUE(t.isDeleted());
}

void test_table() {
    std::cout << "Running test_table..." << std::endl;
    Attribute attr;
    attr.num = 2;
    attr.name[0] = "id";
    attr.type[0] = -1;
    attr.unique[0] = true;
    attr.name[1] = "name";
    attr.type[1] = 10;
    attr.unique[1] = false;
    attr.primary_key = 0;

    Table table("students", attr);
    ASSERT_EQ(table.getTitle(), std::string("students"));
    ASSERT_EQ(table.getAttr().num, 2);
    ASSERT_EQ(table.gethasKey(), 0);

    // Test index operations
    int res = table.setIndex(1, "name_idx");
    ASSERT_EQ(res, 1);
    ASSERT_EQ(table.getIndex().num, 1);
    ASSERT_EQ(table.getIndex().indexname[0], std::string("name_idx"));

    res = table.dropIndex("name_idx");
    ASSERT_EQ(res, 1);
    ASSERT_EQ(table.getIndex().num, 0);
}

int main() {
    test_data_structure();
    test_tuple();
    test_table();
    TEST_REPORT();
}
