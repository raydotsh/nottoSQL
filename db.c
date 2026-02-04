#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
  ib->input_length = 0;
  return ib;
}

void read_input(InputBuffer* ib) {
  ssize_t bytes_read = getline(&(ib->buffer), &(ib->buffer_length), stdin);
  if (bytes_read <= 0) exit(EXIT_FAILURE);
  ib->input_length = bytes_read - 1;
  ib->buffer[bytes_read - 1] = 0;
}

void print_prompt() { printf("db > "); }

/* ================= ROW ================= */

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row;

const uint32_t ID_SIZE = sizeof(uint32_t);
const uint32_t USERNAME_SIZE = COLUMN_USERNAME_SIZE + 1;
const uint32_t EMAIL_SIZE = COLUMN_EMAIL_SIZE + 1;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/* ================= PAGER ================= */

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void* pages[TABLE_MAX_PAGES];
} Pager;

/* ================= TABLE ================= */

typedef struct {
  Pager* pager;
  uint32_t root_page_num;
} Table;

/* ================= CURSOR ================= */

typedef struct {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;
} Cursor;

/* ================= NODE LAYOUT ================= */

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE =
    LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;

const uint32_t LEAF_NODE_SPACE_FOR_CELLS =
    PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

/* ================= NODE ACCESS ================= */

uint32_t* leaf_node_num_cells(void* node) {
  return (uint32_t*)((char*)node + COMMON_NODE_HEADER_SIZE);
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return (char*)node + LEAF_NODE_HEADER_SIZE +
         cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
  return (uint32_t*)leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
  return (char*)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node) {
  *leaf_node_num_cells(node) = 0;
}

/* ================= SERIALIZATION ================= */

void serialize_row(Row* src, void* dest) {
  memcpy(dest, &(src->id), ID_SIZE);
  memcpy((char*)dest + ID_SIZE, &(src->username), USERNAME_SIZE);
  memcpy((char*)dest + ID_SIZE + USERNAME_SIZE, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void* src, Row* dest) {
  memcpy(&(dest->id), src, ID_SIZE);
  memcpy(&(dest->username), (char*)src + ID_SIZE, USERNAME_SIZE);
  memcpy(&(dest->email),
         (char*)src + ID_SIZE + USERNAME_SIZE,
         EMAIL_SIZE);
}

/* ================= PAGER ================= */

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = file_length / PAGE_SIZE;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    pager->pages[i] = NULL;

  return pager;
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    void* page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);

    uint32_t num_pages = pager->file_length / PAGE_SIZE;
    if (page_num < num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      read(pager->file_descriptor, page, PAGE_SIZE);
    }

    pager->pages[page_num] = page;
    if (page_num >= pager->num_pages)
      pager->num_pages = page_num + 1;
  }
  return pager->pages[page_num];
}

void pager_flush(Pager* pager, uint32_t page_num) {
  if (!pager->pages[page_num]) return;
  lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
}

/* ================= DB ================= */

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  if (pager->num_pages == 0) {
    void* root = get_page(pager, 0);
    initialize_leaf_node(root);
  }
  return table;
}

void db_close(Table* table) {
  Pager* pager = table->pager;
  for (uint32_t i = 0; i < pager->num_pages; i++) {
    if (pager->pages[i]) {
      pager_flush(pager, i);
      free(pager->pages[i]);
    }
  }
  close(pager->file_descriptor);
  free(pager);
  free(table);
}

/* ================= CURSOR ================= */

Cursor* table_start(Table* table) {
  Cursor* c = malloc(sizeof(Cursor));
  c->table = table;
  c->page_num = table->root_page_num;
  c->cell_num = 0;

  void* root = get_page(table->pager, table->root_page_num);
  c->end_of_table = (*leaf_node_num_cells(root) == 0);
  return c;
}

Cursor* table_end(Table* table) {
  Cursor* c = malloc(sizeof(Cursor));
  c->table = table;
  c->page_num = table->root_page_num;
  void* root = get_page(table->pager, table->root_page_num);
  c->cell_num = *leaf_node_num_cells(root);
  c->end_of_table = true;
  return c;
}

void cursor_advance(Cursor* c) {
  void* node = get_page(c->table->pager, c->page_num);
  c->cell_num++;
  if (c->cell_num >= *leaf_node_num_cells(node))
    c->end_of_table = true;
}

void* cursor_value(Cursor* c) {
  void* page = get_page(c->table->pager, c->page_num);
  return leaf_node_value(page, c->cell_num);
}

/* ================= LEAF INSERT ================= */

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (cursor->cell_num < num_cells) {
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i),
             leaf_node_cell(node, i - 1),
             LEAF_NODE_CELL_SIZE);
    }
  }

  (*leaf_node_num_cells(node))++;
  *leaf_node_key(node, cursor->cell_num) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

/* ================= META ================= */

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void* node) {
  uint32_t num_cells = *leaf_node_num_cells(node);
  printf("leaf (size %d)\n", num_cells);
  for (uint32_t i = 0; i < num_cells; i++)
    printf("  - %d : %d\n", i, *leaf_node_key(node, i));
}

/* ================= MAIN ================= */

int main(int argc, char* argv[]) {
  Table* table = db_open(argv[1]);
  InputBuffer* ib = new_input_buffer();

  while (true) {
    print_prompt();
    read_input(ib);

    if (strcmp(ib->buffer, ".exit") == 0) {
      db_close(table);
      exit(EXIT_SUCCESS);
    } else if (strcmp(ib->buffer, ".constants") == 0) {
      printf("Constants:\n");
      print_constants();
      continue;
    } else if (strcmp(ib->buffer, ".btree") == 0) {
      printf("Tree:\n");
      print_leaf_node(get_page(table->pager, 0));
      continue;
    }

    if (strncmp(ib->buffer, "insert", 6) == 0) {
      Row row;
      sscanf(ib->buffer, "insert %d %s %s", &row.id, row.username, row.email);

      void* root = get_page(table->pager, table->root_page_num);
      if (*leaf_node_num_cells(root) >= LEAF_NODE_MAX_CELLS) {
        printf("Error: Table full.\n");
        continue;
      }

      Cursor* c = table_end(table);
      leaf_node_insert(c, row.id, &row);
      free(c);
      printf("Executed.\n");
    } else if (strcmp(ib->buffer, "select") == 0) {
      Cursor* c = table_start(table);
      Row row;
      while (!c->end_of_table) {
        deserialize_row(cursor_value(c), &row);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        cursor_advance(c);
      }
      free(c);
      printf("Executed.\n");
    }
  }
}
