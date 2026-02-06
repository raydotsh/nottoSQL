// db.c — Part 10

#include <errno.h>
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

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

const uint32_t ID_SIZE = sizeof(uint32_t);
const uint32_t USERNAME_SIZE = COLUMN_USERNAME_SIZE + 1;
const uint32_t EMAIL_SIZE = COLUMN_EMAIL_SIZE + 1;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/* ================= INPUT ================= */

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
  InputBuffer* ib = malloc(sizeof(InputBuffer));
  ib->buffer = NULL;
  ib->buffer_length = 0;
  return ib;
}

void read_input(InputBuffer* ib) {
  ssize_t bytes_read = getline(&(ib->buffer), &(ib->buffer_length), stdin);
  ib->input_length = bytes_read - 1;
  ib->buffer[bytes_read - 1] = 0;
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
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
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

/* ================= NODE ================= */

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_OFFSET = 1;
const uint32_t PARENT_POINTER_OFFSET = 2;
const uint32_t COMMON_NODE_HEADER_SIZE = 6;

uint8_t get_node_type(void* node) { return *((uint8_t*)(node + NODE_TYPE_OFFSET)); }
void set_node_type(void* node, NodeType type) { *((uint8_t*)(node + NODE_TYPE_OFFSET)) = type; }
bool is_node_root(void* node) { return *((uint8_t*)(node + IS_ROOT_OFFSET)); }
void set_node_root(void* node, bool is_root) { *((uint8_t*)(node + IS_ROOT_OFFSET)) = is_root; }

/* ================= LEAF ================= */

const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + 4;
const uint32_t LEAF_NODE_CELL_SIZE = sizeof(uint32_t) + ROW_SIZE;
const uint32_t LEAF_NODE_SPACE = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE / LEAF_NODE_CELL_SIZE;

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT =
    (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

uint32_t* leaf_node_num_cells(void* node) { return node + LEAF_NODE_NUM_CELLS_OFFSET; }
void* leaf_node_cell(void* node, uint32_t i) { return node + LEAF_NODE_HEADER_SIZE + i * LEAF_NODE_CELL_SIZE; }
uint32_t* leaf_node_key(void* node, uint32_t i) { return leaf_node_cell(node, i); }
void* leaf_node_value(void* node, uint32_t i) { return leaf_node_cell(node, i) + sizeof(uint32_t); }

void initialize_leaf_node(void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
}

/* ================= INTERNAL ================= */

const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = COMMON_NODE_HEADER_SIZE + 4;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + 8;
const uint32_t INTERNAL_NODE_CELL_SIZE = 8;

uint32_t* internal_node_num_keys(void* node) { return node + INTERNAL_NODE_NUM_KEYS_OFFSET; }
uint32_t* internal_node_right_child(void* node) { return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET; }
uint32_t* internal_node_cell(void* node, uint32_t i) { return node + INTERNAL_NODE_HEADER_SIZE + i * INTERNAL_NODE_CELL_SIZE; }
uint32_t* internal_node_child(void* node, uint32_t i) { return internal_node_cell(node, i); }
uint32_t* internal_node_key(void* node, uint32_t i) { return internal_node_cell(node, i) + 4; }

void initialize_internal_node(void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;
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
    initialize_leaf_node(root);
    set_node_root(root, true);
  }
  return table;
}

/* ================= CURSOR ================= */

typedef struct { Table* table; uint32_t page_num; uint32_t cell_num; } Cursor;

Cursor* table_find(Table* table, uint32_t key) {
  void* root = get_page(table->pager, table->root_page_num);
  if (get_node_type(root) == NODE_INTERNAL) {
    printf("Need to implement searching an internal node\n");
    exit(EXIT_FAILURE);
  }
  Cursor* c = malloc(sizeof(Cursor));
  c->table = table;
  c->page_num = table->root_page_num;
  c->cell_num = *leaf_node_num_cells(root);
  return c;
}

/* ================= SPLIT + ROOT ================= */

uint32_t get_node_max_key(void* node) {
  if (get_node_type(node) == NODE_INTERNAL)
    return *internal_node_key(node, *internal_node_num_keys(node) - 1);
  return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
}

void create_new_root(Table* table, uint32_t right_page);

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t new_page = get_unused_page_num(cursor->table->pager);
  void* new_node = get_page(cursor->table->pager, new_page);
  initialize_leaf_node(new_node);

  for (int i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
    void* dest = (i >= LEAF_NODE_LEFT_SPLIT_COUNT) ? new_node : old_node;
    uint32_t index = i % LEAF_NODE_LEFT_SPLIT_COUNT;

    if (i == cursor->cell_num) {
      memcpy(leaf_node_value(dest, index), value, ROW_SIZE);
      *leaf_node_key(dest, index) = key;
    } else if (i > cursor->cell_num) {
      memcpy(leaf_node_cell(dest, index), leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
    } else {
      memcpy(leaf_node_cell(dest, index), leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }
  }

  *leaf_node_num_cells(old_node) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *leaf_node_num_cells(new_node) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) create_new_root(cursor->table, new_page);
  else { printf("Need to implement updating parent after split\n"); exit(EXIT_FAILURE); }
}

void create_new_root(Table* table, uint32_t right_page) {
  void* root = get_page(table->pager, table->root_page_num);
  void* right = get_page(table->pager, right_page);
  uint32_t left_page = get_unused_page_num(table->pager);
  void* left = get_page(table->pager, left_page);

  memcpy(left, root, PAGE_SIZE);
  set_node_root(left, false);

  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_page;
  *internal_node_key(root, 0) = get_node_max_key(left);
  *internal_node_right_child(root) = right_page;
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

      if (*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)
        leaf_node_split_and_insert(c, row.id, &row);
      else {
        *leaf_node_key(node, c->cell_num) = row.id;
        memcpy(leaf_node_value(node, c->cell_num), &row, ROW_SIZE);
        (*leaf_node_num_cells(node))++;
      }

      printf("Executed.\n");
    }
  }
}
