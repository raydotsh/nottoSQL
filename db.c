// db.c — Part 11

#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100

/* ================= ROW ================= */

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

const uint32_t ROW_SIZE = sizeof(Row);

/* ================= INPUT ================= */

typedef struct {
  char* buffer;
  size_t buffer_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
  InputBuffer* ib = malloc(sizeof(InputBuffer));
  ib->buffer = NULL;
  ib->buffer_length = 0;
  return ib;
}

void read_input(InputBuffer* ib) {
  getline(&(ib->buffer), &(ib->buffer_length), stdin);
  ib->buffer[strcspn(ib->buffer, "\n")] = 0;
}

void print_prompt() { printf("db > "); }

/* ================= PAGER ================= */

typedef struct {
  int fd;
  uint32_t file_length;
  uint32_t num_pages;
  void* pages[TABLE_MAX_PAGES];
} Pager;

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, 0600);
  off_t len = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->fd = fd;
  pager->file_length = len;
  pager->num_pages = len / PAGE_SIZE;

  for (int i = 0; i < TABLE_MAX_PAGES; i++) pager->pages[i] = NULL;
  return pager;
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (!pager->pages[page_num]) {
    void* page = calloc(1, PAGE_SIZE);
    pager->pages[page_num] = page;
    if (page_num >= pager->num_pages) pager->num_pages = page_num + 1;
  }
  return pager->pages[page_num];
}

uint32_t get_unused_page_num(Pager* pager) { return pager->num_pages; }

/* ================= NODE HEADERS ================= */

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

#define NODE_TYPE_OFFSET 0
#define IS_ROOT_OFFSET 1
#define COMMON_HEADER_SIZE 6

uint8_t get_node_type(void* node) { return *((uint8_t*)(node + NODE_TYPE_OFFSET)); }
void set_node_type(void* node, NodeType type) { *((uint8_t*)(node + NODE_TYPE_OFFSET)) = type; }
bool is_node_root(void* node) { return *((uint8_t*)(node + IS_ROOT_OFFSET)); }
void set_node_root(void* node, bool is_root) { *((uint8_t*)(node + IS_ROOT_OFFSET)) = is_root; }

/* ================= LEAF ================= */

#define LEAF_NUM_CELLS_OFFSET COMMON_HEADER_SIZE
#define LEAF_HEADER_SIZE (COMMON_HEADER_SIZE + 4)
#define LEAF_CELL_SIZE (sizeof(uint32_t) + ROW_SIZE)
#define LEAF_MAX_CELLS ((PAGE_SIZE - LEAF_HEADER_SIZE) / LEAF_CELL_SIZE)

const uint32_t LEAF_RIGHT_SPLIT = (LEAF_MAX_CELLS + 1) / 2;
const uint32_t LEAF_LEFT_SPLIT = (LEAF_MAX_CELLS + 1) - LEAF_RIGHT_SPLIT;

uint32_t* leaf_num_cells(void* node) { return node + LEAF_NUM_CELLS_OFFSET; }
void* leaf_cell(void* node, uint32_t i) { return node + LEAF_HEADER_SIZE + i * LEAF_CELL_SIZE; }
uint32_t* leaf_key(void* node, uint32_t i) { return leaf_cell(node, i); }
void* leaf_value(void* node, uint32_t i) { return leaf_cell(node, i) + sizeof(uint32_t); }

void initialize_leaf(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_num_cells(node) = 0;
}

/* ================= INTERNAL ================= */

#define INTERNAL_NUM_KEYS_OFFSET COMMON_HEADER_SIZE
#define INTERNAL_RIGHT_CHILD_OFFSET (COMMON_HEADER_SIZE + 4)
#define INTERNAL_HEADER_SIZE (COMMON_HEADER_SIZE + 8)
#define INTERNAL_CELL_SIZE 8

uint32_t* internal_num_keys(void* node) { return node + INTERNAL_NUM_KEYS_OFFSET; }
uint32_t* internal_right_child(void* node) { return node + INTERNAL_RIGHT_CHILD_OFFSET; }
uint32_t* internal_cell(void* node, uint32_t i) { return node + INTERNAL_HEADER_SIZE + i * INTERNAL_CELL_SIZE; }
uint32_t* internal_child(void* node, uint32_t i) { return internal_cell(node, i); }
uint32_t* internal_key(void* node, uint32_t i) { return internal_cell(node, i) + 4; }

void initialize_internal(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_num_keys(node) = 0;
}

/* ================= TABLE ================= */

typedef struct { Pager* pager; uint32_t root_page_num; } Table;

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  if (pager->num_pages == 0) {
    void* root = get_page(pager, 0);
    initialize_leaf(root);
    set_node_root(root, true);
  }
  return table;
}

/* ================= SEARCH ================= */

typedef struct { Table* table; uint32_t page_num; uint32_t cell_num; } Cursor;

Cursor* leaf_find(Table* table, uint32_t page_num, uint32_t key) {
  Cursor* c = malloc(sizeof(Cursor));
  c->table = table;
  c->page_num = page_num;
  c->cell_num = *leaf_num_cells(get_page(table->pager, page_num));
  return c;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);

Cursor* table_find(Table* table, uint32_t key) {
  void* root = get_page(table->pager, table->root_page_num);
  if (get_node_type(root) == NODE_LEAF)
    return leaf_find(table, table->root_page_num, key);
  return internal_node_find(table, table->root_page_num, key);
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_keys = *internal_num_keys(node);

  uint32_t min = 0;
  uint32_t max = num_keys;

  while (min != max) {
    uint32_t i = (min + max) / 2;
    if (*internal_key(node, i) >= key) max = i;
    else min = i + 1;
  }

  uint32_t child_page = *internal_child(node, min);
  void* child = get_page(table->pager, child_page);

  if (get_node_type(child) == NODE_LEAF)
    return leaf_find(table, child_page, key);
  else
    return internal_node_find(table, child_page, key);
}

/* ================= SPLIT STUB (same as Part 10) ================= */

void leaf_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
  printf("Need to implement updating parent after split\n");
  exit(EXIT_FAILURE);
}

/* ================= MAIN ================= */

int main(int argc, char* argv[]) {
  setbuf(stdout, NULL);

  Table* table = db_open(argv[1]);
  InputBuffer* ib = new_input_buffer();

  while (true) {
    print_prompt();
    read_input(ib);

    if (strcmp(ib->buffer, ".exit") == 0) exit(0);

    if (strncmp(ib->buffer, "insert", 6) == 0) {
      Row row;
      sscanf(ib->buffer, "insert %d %s %s", &row.id, row.username, row.email);
      Cursor* c = table_find(table, row.id);
      void* node = get_page(table->pager, c->page_num);

      if (*leaf_num_cells(node) >= LEAF_MAX_CELLS)
        leaf_split_and_insert(c, row.id, &row);
      else {
        *leaf_key(node, c->cell_num) = row.id;
        memcpy(leaf_value(node, c->cell_num), &row, sizeof(Row));
        (*leaf_num_cells(node))++;
      }

      printf("Executed.\n");
    }
  }
}
