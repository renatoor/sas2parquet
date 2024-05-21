#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <cstdlib>
#include <cstring>
#include <readstat.h>
#include <memory>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

typedef struct mmap_io_ctx_s {
  int fd;
  char *addr;
  char *curr_addr;
  off_t offset;
  size_t length;
} mmap_io_ctx_t;

int mmap_open_handler(const char *path, void *io_ctx) {
  int fd = -1;
  off_t offset = 0;
  struct stat sb;

  fd = open(path, O_RDONLY);
  fstat(fd, &sb);

  char *addr = (char *) mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, fd, offset);

  mmap_io_ctx_t *mmap_io_ctx = (mmap_io_ctx_t *) io_ctx;

  mmap_io_ctx->fd = fd;
  mmap_io_ctx->addr = addr;
  mmap_io_ctx->curr_addr = addr;
  mmap_io_ctx->offset = offset;
  mmap_io_ctx->length = sb.st_size;

  return fd;
}

readstat_off_t mmap_seek_handler(readstat_off_t offset, readstat_io_flags_t whence, void *io_ctx) {
  mmap_io_ctx_t *mmap_io_ctx = (mmap_io_ctx_t *) io_ctx;

  switch(whence) {
    case READSTAT_SEEK_SET:
      mmap_io_ctx->curr_addr = mmap_io_ctx->addr + offset;
      mmap_io_ctx->offset = offset;
      break;
    case READSTAT_SEEK_CUR:
      mmap_io_ctx->curr_addr += offset;
      mmap_io_ctx->offset += offset;
      break;
    case READSTAT_SEEK_END:
      mmap_io_ctx->curr_addr = mmap_io_ctx->addr + mmap_io_ctx->length;
      mmap_io_ctx->offset = mmap_io_ctx->length;
      break;
    default:
      return -1;
  }

  return mmap_io_ctx->offset;
}

ssize_t mmap_read_handler(void *buf, size_t nbyte, void *io_ctx) {
  mmap_io_ctx_t *mmap_io_ctx = (mmap_io_ctx_t *) io_ctx;

  memcpy(buf, mmap_io_ctx->curr_addr, nbyte);

  mmap_io_ctx->offset += nbyte;
  mmap_io_ctx->curr_addr += nbyte;

  return nbyte;
}

int mmap_close_handler(void *io_ctx) {
  mmap_io_ctx_t *mmap_io_ctx = (mmap_io_ctx_t *) io_ctx;

  munmap(mmap_io_ctx->addr, mmap_io_ctx->length);
  close(mmap_io_ctx->fd);

  return 0;
}

readstat_error_t mmap_io_init(readstat_parser_t *parser) {
  readstat_set_open_handler(parser, mmap_open_handler);
  readstat_set_close_handler(parser, mmap_close_handler);
  readstat_set_read_handler(parser, mmap_read_handler);
  readstat_set_seek_handler(parser, mmap_seek_handler);


  mmap_io_ctx_t *io_ctx = (mmap_io_ctx_t *) calloc(1, sizeof(mmap_io_ctx_t));

  io_ctx->fd = -1;
  io_ctx->addr = NULL;
  io_ctx->length = 0;

  readstat_set_io_ctx(parser, (void *) io_ctx);
  parser->io->io_ctx_needs_free = 1;

  return READSTAT_OK;
}

class Parser {
public:

  Parser() {
    _parser = readstat_parser_init();

    mmap_io_init(_parser);

    readstat_set_metadata_handler(_parser, [] (readstat_metadata_t *metadata, void *ctx) -> int {
      auto self = (Parser *) ctx;
      return self->HandleMetadata(metadata);
    });

    readstat_set_variable_handler(_parser, [] (int index, readstat_variable_t *variable, const char *val_labels, void *ctx) -> int {
      auto self = (Parser *) ctx;
      auto status = self->HandleVariable(index, variable, val_labels);

      if (status.ok()) {
        return READSTAT_HANDLER_OK;
      }

      return -1;
    });

    readstat_set_value_handler(_parser, [] (int obs_index, readstat_variable_t *variable, readstat_value_t value, void *ctx) -> int {
      auto self = (Parser *) ctx;
      auto status = self->HandleValue(obs_index, variable, value);

      if (status.ok()) {
        return READSTAT_HANDLER_OK;
      }

      return -1;
    });
  }

  ~Parser() {
    readstat_parser_free(_parser);
  }

  void Parse(char *filename) {
    auto error = readstat_parse_sas7bdat(_parser, filename, this);

    if (error != READSTAT_OK) {
        printf("Error processing %s: %d\n", filename, error);
    }
  }

  arrow::Status Finish() {
    ARROW_ASSIGN_OR_RAISE(_batch, _batch_builder->Flush());
    ARROW_RETURN_NOT_OK(_batch->ValidateFull());

    return arrow::Status::OK();
  }

  arrow::Status WriteParquet(const char *filename) {
    auto props = parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();

    ARROW_ASSIGN_OR_RAISE(
      auto outfile, arrow::io::FileOutputStream::Open(filename));

    ARROW_ASSIGN_OR_RAISE(
      auto writer, parquet::arrow::FileWriter::Open(*_batch->schema().get(), arrow::default_memory_pool(), outfile, props));

    ARROW_ASSIGN_OR_RAISE(
      auto table, arrow::Table::FromRecordBatches(_batch->schema(), {_batch}));

    ARROW_RETURN_NOT_OK(writer->WriteTable(*table.get(), _batch->num_rows()));

    ARROW_RETURN_NOT_OK(writer->Close());

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DataType& type) {
    return arrow::Status::NotImplemented(
        "Cannot convert value to Arrow array of type ", type.ToString());
  }

  arrow::Status Visit(const arrow::Int8Type &type) {
    auto builder = static_cast<arrow::Int8Builder *>(_current_builder);

    if (readstat_value_is_missing(*_current_value, _current_variable)) {
      ARROW_RETURN_NOT_OK(builder->AppendNull());
    }
    else {
      ARROW_RETURN_NOT_OK(builder->Append(readstat_int8_value(*_current_value)));
    }

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &type) {
    auto builder = static_cast<arrow::Int16Builder *>(_current_builder);

    if (readstat_value_is_missing(*_current_value, _current_variable)) {
      ARROW_RETURN_NOT_OK(builder->AppendNull());
    }
    else {
      ARROW_RETURN_NOT_OK(builder->Append(readstat_int16_value(*_current_value)));
    }

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &type) {
    auto builder = static_cast<arrow::Int32Builder *>(_current_builder);

    if (readstat_value_is_missing(*_current_value, _current_variable)) {
      ARROW_RETURN_NOT_OK(builder->AppendNull());
    }
    else {
      ARROW_RETURN_NOT_OK(builder->Append(readstat_int32_value(*_current_value)));
    }

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &type) {
    auto builder = static_cast<arrow::FloatBuilder *>(_current_builder);

    if (readstat_value_is_missing(*_current_value, _current_variable)) {
      ARROW_RETURN_NOT_OK(builder->AppendNull());
    }
    else {
      ARROW_RETURN_NOT_OK(builder->Append(readstat_float_value(*_current_value)));
    }

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &type) {
    auto builder = static_cast<arrow::DoubleBuilder *>(_current_builder);

    if (readstat_value_is_missing(*_current_value, _current_variable)) {
      ARROW_RETURN_NOT_OK(builder->AppendNull());
    }
    else {
      ARROW_RETURN_NOT_OK(builder->Append(readstat_double_value(*_current_value)));
    }

    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &type) {
    auto builder = static_cast<arrow::StringBuilder *>(_current_builder);

    ARROW_RETURN_NOT_OK(builder->Append(readstat_string_value(*_current_value)));

    return arrow::Status::OK();
  }

private:

  int HandleMetadata(readstat_metadata_t *metadata) {
    _row_count = readstat_get_row_count(metadata);
    _variable_count = readstat_get_var_count(metadata);

    _fields.reserve(_variable_count);

    return READSTAT_HANDLER_OK;
  }

  arrow::Status HandleVariable(int index, readstat_variable_t *variable, const char *val_labels) {
    auto type = GetArrowDataType(readstat_variable_get_type(variable));
    auto name = readstat_variable_get_name(variable);

    _fields.push_back(arrow::field(name, type));

    if (index == _variable_count - 1) {
      _schema = arrow::schema(_fields);

      ARROW_ASSIGN_OR_RAISE(
        _batch_builder,
        arrow::RecordBatchBuilder::Make(_schema, arrow::default_memory_pool(), _row_count)
      );
    }

    return arrow::Status::OK();
  }

  arrow::Status HandleValue(int obs_index, readstat_variable_t *variable, readstat_value_t value) {
    int index = readstat_variable_get_index(variable);
    auto field = _schema->field(index);

    _current_value = &value;
    _current_variable = variable;
    _current_builder = _batch_builder->GetField(index);

    ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field->type().get(), this));

    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::DataType> GetArrowDataType(readstat_type_t type) {
    switch (type) {
      case READSTAT_TYPE_STRING_REF:
      case READSTAT_TYPE_STRING:
        return arrow::utf8();
      case READSTAT_TYPE_INT8:
        return arrow::int8();
      case READSTAT_TYPE_INT16:
        return arrow::int16();
      case READSTAT_TYPE_INT32:
        return arrow::int32();
      case READSTAT_TYPE_FLOAT:
        return arrow::float32();
      case READSTAT_TYPE_DOUBLE:
        return arrow::float64();
    }
  }

  readstat_parser_t *_parser;
  readstat_value_t *_current_value;
  readstat_variable_t *_current_variable;

  int _row_count;
  int _variable_count;

  arrow::FieldVector _fields;
  std::shared_ptr<arrow::Schema> _schema;
  std::unique_ptr<arrow::RecordBatchBuilder> _batch_builder;
  arrow::ArrayBuilder *_current_builder;
  std::shared_ptr<arrow::RecordBatch> _batch;
};

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Usage: %s <filename> <parquet_filename>\n", argv[0]);
        return 1;
    }

    auto parser = Parser();

    parser.Parse(argv[1]);

    auto status = parser.Finish();
    auto status2 = parser.WriteParquet(argv[2]);

    return 0;
}
