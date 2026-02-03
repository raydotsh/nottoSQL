#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ================= INPUT BUFFER ================= */

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

void close_input_buffer(InputBuffer* ib) {
  free(ib->buffer);
  free(ib);
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* ib) {
  ssize_t bytes_read = getline(&(ib->buffer), &(ib->buffer_length), stdin);
  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }
  ib->input_length = bytes_read - 1;
  ib->buffer[bytes_read - 1] = 0;
}

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

const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/* ================= TABLE CONSTANTS ================= */

const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

/* ================= PAGER ================= */

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  void* pages[TABLE_MAX_PAGES];
} Pager;

/* ================= TABLE ================= */

typedef struct {
  Pager* pager;
  uint32_t num_rows;
} Table;

/* ================= CURSOR ================= */

typedef struct {
  Table* table;
  uint32_t row_num;
  bool end_of_table;
} Cursor;

/* ================= SERIALIZATION ================= */

void serialize_row(Row* src, void* dest) {
  memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);
  memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);
  memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void* src, Row* dest) {
  memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);
  memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

/* ================= PAGER FUNCTIONS ================= */

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++)
    pager->pages[i] = NULL;

  return pager;
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num >= TABLE_MAX_PAGES) {
    printf("Page out of bounds\n");
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    void* page = malloc(PAGE_SIZE);

    uint32_t num_pages = pager->file_length / PAGE_SIZE;
    if (pager->file_length % PAGE_SIZE) num_pages++;

    if (page_num < num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      read(pager->file_descriptor, page, PAGE_SIZE);
    }

    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) return;

  lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  write(pager->file_descriptor, pager->pages[page_num], size);
}

/* ================= TABLE FUNCTIONS ================= */

Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  uint32_t num_rows = pager->file_length / ROW_SIZE;

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->num_rows = num_rows;
  return table;
}

void db_close(Table* table) {
  Pager* pager = table->pager;

  uint32_t full_pages = table->num_rows / ROWS_PER_PAGE;
  for (uint32_t i = 0; i < full_pages; i++) {
    if (pager->pages[i])
      pager_flush(pager, i, PAGE_SIZE);
  }

  uint32_t leftover_rows = table->num_rows % ROWS_PER_PAGE;
  if (leftover_rows) {
    uint32_t page_num = full_pages;
    pager_flush(pager, page_num, leftover_rows * ROW_SIZE);
  }

  close(pager->file_descriptor);
  free(pager);
  free(table);
}

/* ================= CURSOR FUNCTIONS ================= */

Cursor* table_start(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = 0;
  cursor->end_of_table = (table->num_rows == 0);
  return cursor;
}

Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = table->num_rows;
  cursor->end_of_table = true;
  return cursor;
}

void* cursor_value(Cursor* cursor) {
  uint32_t row_num = cursor->row_num;
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = get_page(cursor->table->pager, page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
  cursor->row_num++;
  if (cursor->row_num >= cursor->table->num_rows)
    cursor->end_of_table = true;
}

/* ================= STATEMENTS ================= */

typedef enum { META_COMMAND_SUCCESS, META_COMMAND_UNRECOGNIZED } MetaResult;
typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED } PrepareResult;
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;
typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;

typedef struct {
  StatementType type;
  Row row;
} Statement;

/* ================= META ================= */

MetaResult do_meta_command(InputBuffer* ib, Table* table) {
  if (strcmp(ib->buffer, ".exit") == 0) {
    close_input_buffer(ib);
    db_close(table);
    exit(EXIT_SUCCESS);
  }
  return META_COMMAND_UNRECOGNIZED;
}

/* ================= PREPARE ================= */

PrepareResult prepare_statement(InputBuffer* ib, Statement* st) {
  if (strncmp(ib->buffer, "insert", 6) == 0) {
    st->type = STATEMENT_INSERT;
    sscanf(ib->buffer, "insert %d %s %s",
           &(st->row.id), st->row.username, st->row.email);
    return PREPARE_SUCCESS;
  }
  if (strcmp(ib->buffer, "select") == 0) {
    st->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED;
}

/* ================= EXECUTE ================= */

ExecuteResult execute_statement(Statement* st, Table* table) {

  if (st->type == STATEMENT_INSERT) {
    if (table->num_rows >= TABLE_MAX_ROWS)
      return EXECUTE_TABLE_FULL;

    Cursor* cursor = table_end(table);
    serialize_row(&st->row, cursor_value(cursor));
    table->num_rows++;
    free(cursor);

    return EXECUTE_SUCCESS;
  }

  if (st->type == STATEMENT_SELECT) {
    Cursor* cursor = table_start(table);
    Row row;

    while (!cursor->end_of_table) {
      deserialize_row(cursor_value(cursor), &row);
      printf("(%d, %s, %s)\n", row.id, row.username, row.email);
      cursor_advance(cursor);
    }

    free(cursor);
    return EXECUTE_SUCCESS;
  }

  return EXECUTE_SUCCESS;
}

/* ================= MAIN ================= */

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  Table* table = db_open(argv[1]);
  InputBuffer* ib = new_input_buffer();

  while (true) {
    print_prompt();
    read_input(ib);

    if (ib->buffer[0] == '.') {
      do_meta_command(ib, table);
      continue;
    }

    Statement st;
    if (prepare_statement(ib, &st) == PREPARE_UNRECOGNIZED) {
      printf("Unrecognized command\n");
      continue;
    }

    if (execute_statement(&st, table) == EXECUTE_TABLE_FULL)
      printf("Error: Table full.\n");
    else
      printf("Executed.\n");
  }
}
