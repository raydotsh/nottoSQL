## nottoSQL

A bare-bones SQLite-style database engine built from scratch in C.

This is a minimal, experimental SQL-like database implementation created as a learning project. The goal is to understand how databases work internally.

---

### Features

Pure C implementation

Basic database engine structure

Spec included

---

### Project Structure
```
nottoSQL/
├── db.c          # Core database engine implementation
├── spec          # Design notes / project specification
├── test.db       # Sample database file
├── Gemfile       # Repo tooling (not required for C build)
├── Gemfile.lock
├── .gitignore
└── README.md
```
---

### Requirements

GCC or Clang

Unix-like environment recommended (Linux/macOS/WSL)

---

### Build

gcc db.c -o nottoSQL

---

Run

./db

Output will be minimal unless additional logic (tests, REPL, etc.) is added.

---

### Purpose

This project is for exploring:

How databases store data on disk

Query parsing concepts

Table/page structure basics

Low-level systems programming in C


This is not a SQLite clone and not meant for production use.

If you need a real database, use SQLite, PostgreSQL, MySQL, etc or wait until I get back to this.

---

### Testing

No formal test suite yet.

You can:

Modify db.c

Use test.db

Add your own test cases.


Test contributions are welcome.

---

### Contributing

1. Fork the repo

2. Make your changes

3. Keep the code readable

4. Open a PR

---

### License 

MIT
