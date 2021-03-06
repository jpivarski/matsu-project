#include "GeoPictureSerializer.h"
#include <sstream>
#include <math.h>
#include <stdio.h>
#include <avro/Stream.hh>
#include <time.h>

/*
 * PyVarObject_HEAD_INIT was added in Python 2.6.  Its use is
 * necessary to handle both Python 2 and 3.  This replacement
 * definition is for Python <= 2.5
 */
#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(type, size) \
    PyObject_HEAD_INIT(type) size,
#endif

#ifndef Py_TYPE
#define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#if PY_MAJOR_VERSION >= 3
    #define MOD_DEF(ob, name, doc, methods) \
        static struct PyModuleDef moduledef = { \
            PyModuleDef_HEAD_INIT, name, doc, -1, methods, }; \
        ob = PyModule_Create(&moduledef);
#else
    #define MOD_DEF(ob, name, doc, methods) \
        ob = Py_InitModule3(name, methods, doc);
#endif

/*
 * Python 3 only has long.
 */
#if PY_MAJOR_VERSION >= 3
#define PyInt_AsLong PyLong_AsLong
#define PyInt_Check PyLong_Check
#endif

#if PY_MAJOR_VERSION >= 3
#define PyString_Check(x) 1
#define PyString_FromString(x) PyUnicode_FromString(x)
#define PyString_FromFormat(x,y) PyUnicode_FromFormat(x,y)
#define PyString_AsString(x) PyUnicode_AS_DATA(x)
#endif

static PyMemberDef GeoPictureSerializer_GeoPicture_members[] = {
  {"schema", T_OBJECT, offsetof(GeoPictureSerializer_GeoPicture, schema), READONLY, "The AVRO schema as a JSON string."},
  {"metadata", T_OBJECT, offsetof(GeoPictureSerializer_GeoPicture, metadata), 0, "The picture metadata as a Python dictionary."},
  {"bands", T_OBJECT, offsetof(GeoPictureSerializer_GeoPicture, bands), 0, "The names of the bands as a sequence of strings."},

  {NULL}
};

static PyMethodDef GeoPictureSerializer_GeoPicture_methods[] = {
  {"serialize", (PyCFunction)(GeoPictureSerializer_GeoPicture_serialize), METH_VARARGS, "Return a serialized string representing the data."},

  {NULL}
};

static PyGetSetDef GeoPictureSerializer_GeoPicture_getsetters[] = {
  {"picture", (getter)GeoPictureSerializer_GeoPicture_getPicture, (setter)GeoPictureSerializer_GeoPicture_setPicture, "The picture data as a 3-D NumPy array: height, width, depth (bands).", NULL},

  {NULL}
};

static PyTypeObject GeoPictureSerializer_GeoPictureType = {
   PyVarObject_HEAD_INIT(NULL,0)
   "GeoPictureSerializer.GeoPicture",        /*	tp_name */
   sizeof(GeoPictureSerializer_GeoPicture),  /*	tp_basicsize */
   0,                                        /*	tp_itemsize */
   (destructor)GeoPictureSerializer_GeoPicture_dealloc,  /*	tp_dealloc */
   0,                                        /*	tp_print */
   0,                         		     /*	tp_getattr */
   0,                         		     /*	tp_setattr */
   0,                         		     /*	tp_compare */
   0,                         		     /*	tp_repr */
   0,                         		     /*	tp_as_number */
   0,                         		     /*	tp_as_sequence */
   0,                         		     /*	tp_as_mapping */
   0,                         		     /*	tp_hash */
   0,                         		     /*	tp_call */
   0,                         		     /*	tp_str */
   0,                         		     /*	tp_getattro */
   0,                         		     /*	tp_setattro */
   0,                         		     /*	tp_as_buffer */
   Py_TPFLAGS_DEFAULT,        		     /*	tp_flags */
   "Holder for data to be serialized; contains some raster bands and metadata.", /* tp_doc */
   0,		               		     /* tp_traverse */
   0,		               		     /* tp_clear */
   0,		               		     /* tp_richcompare */
   0,		               		     /* tp_weaklistoffset */
   0,		               		     /* tp_iter */
   0,		               		     /* tp_iternext */
   GeoPictureSerializer_GeoPicture_methods,  /* tp_methods */
   GeoPictureSerializer_GeoPicture_members,  /* tp_members */
   GeoPictureSerializer_GeoPicture_getsetters,  /* tp_getset */
   0,                         		     /* tp_base */
   0,                         		     /* tp_dict */
   0,                         		     /* tp_descr_get */
   0,                         		     /* tp_descr_set */
   0,                         		     /* tp_dictoffset */
   (initproc)GeoPictureSerializer_GeoPicture_init,  /* tp_init */
   0,                                        /* tp_alloc */
   0,                                        /* tp_new */
};

static int GeoPictureSerializer_GeoPicture_init(GeoPictureSerializer_GeoPicture *self) {
  self->schema = PyString_FromString(
"{\"type\": \"record\", \"name\": \"GeoPictureWithMetadata\", \"fields\":\n"
"    [{\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}},\n"
"     {\"name\": \"bands\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
"     {\"name\": \"height\", \"type\": \"int\"},\n"
"     {\"name\": \"width\", \"type\": \"int\"},\n"
"     {\"name\": \"depth\", \"type\": \"int\"},\n"
"     {\"name\": \"dtype\", \"type\": \"int\"},\n"
"     {\"name\": \"itemsize\", \"type\": \"int\"},\n"
"     {\"name\": \"nbytes\", \"type\": \"long\"},\n"
"     {\"name\": \"fortran\", \"type\": \"boolean\"},\n"
"     {\"name\": \"byteorder\", \"type\": {\"type\": \"enum\", \"name\": \"ByteOrder\", \"symbols\": [\"LittleEndian\", \"BigEndian\", \"NativeEndian\", \"IgnoreEndian\"]}},\n"
"     {\"name\": \"data\", \"type\":\n"
"	 {\"type\": \"array\", \"items\":\n"
"	     {\"type\": \"record\", \"name\": \"ZeroSuppressed\", \"fields\":\n"
"		 [{\"name\": \"index\", \"type\": \"long\"}, {\"name\": \"strip\", \"type\": \"bytes\"}]}}}\n"
"     ]}");
  try {
    self->validSchema = avro::compileJsonSchemaFromString(PyString_AsString(self->schema));
  }
  catch (...) {
    PyErr_SetString(PyExc_TypeError, "schema is not valid (non-parsing JSON or missing AVRO fields)");
    return -1;
  }

  self->metadata = PyDict_New();
  self->bands = PyList_New(0);
  self->picture = Py_BuildValue("O", Py_None);
  return 0;
}

static void GeoPictureSerializer_GeoPicture_dealloc(GeoPictureSerializer_GeoPicture *self) {
  Py_XDECREF(self->schema);
  Py_XDECREF(self->metadata);
  Py_XDECREF(self->bands);
  Py_XDECREF(self->picture);
  self->ob_type->tp_free((PyObject*)self);
}

static const char b64encodeTable[65] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const char b64decodeTable[256] = {
  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
  ,-1,62,-1,-1,-1,63,52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
  ,22,23,24,25,-1,-1,-1,-1,-1,-1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
  ,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
  -1,-1,-1}; 

class MyOutputStream : public avro::OutputStream {
public:
  FILE *file_;
  std::stringstream alt_;

  const size_t chunkSize_;
  uint8_t* data_;
  size_t available_;
  size_t byteCount_;
  bool written_;

  char buff1[3];
  uint8_t i;

  MyOutputStream(FILE *f, size_t chunkSize) : file_(f), chunkSize_(chunkSize), available_(0), byteCount_(0), written_(true), i(0) {
    data_ = new uint8_t[chunkSize_];
  }
  ~MyOutputStream() {
    delete [] data_;
  }
  
  bool next(uint8_t** data, size_t* len) {
    if (available_ == 0) {
      flush();
      available_ = chunkSize_;
    }
    *data = &(data_[chunkSize_ - available_]);
    *len = available_;
    byteCount_ += available_;
    available_ = 0;
    written_ = false;

    return true;
  }
  
  void backup(size_t len) {
    available_ += len;
    byteCount_ -= len;
  }
  
  uint64_t byteCount() const {
    return byteCount_;
  }
  
  // void flush() {
  //   if (!written_) {
  //     fwrite(data_, sizeof(uint8_t), chunkSize_ - available_, file_);
  //     written_ = true;
  //   }
  // }

  void flush() {
    if (!written_) {
      for (size_t k = 0;  k < chunkSize_ - available_;  k++) {
	buff1[i++] = data_[k];
	if (i == 3) {
	  if (file_ != NULL) {
	    fputc(b64encodeTable[(buff1[0] & 0xfc) >> 2], file_);
	    fputc(b64encodeTable[((buff1[0] & 0x03) << 4) + ((buff1[1] & 0xf0) >> 4)], file_);
	    fputc(b64encodeTable[((buff1[1] & 0x0f) << 2) + ((buff1[2] & 0xc0) >> 6)], file_);
	    fputc(b64encodeTable[buff1[2] & 0x3f], file_);
	  }
	  else {
	    alt_ << b64encodeTable[(buff1[0] & 0xfc) >> 2];
	    alt_ << b64encodeTable[((buff1[0] & 0x03) << 4) + ((buff1[1] & 0xf0) >> 4)];
	    alt_ << b64encodeTable[((buff1[1] & 0x0f) << 2) + ((buff1[2] & 0xc0) >> 6)];
	    alt_ << b64encodeTable[buff1[2] & 0x3f];
	  }
	  i = 0;
	}
      }
      written_ = true;
    }
  }

  void finishUp() {
    i--;
    if (i > 0  &&  i < 4) {
      for(uint8_t j = i;  j < 3;  j++) { buff1[j] = '\0'; }

      char buff2[4];
      buff2[0] = (buff1[0] & 0xfc) >> 2;
      buff2[1] = ((buff1[0] & 0x03) << 4) + ((buff1[1] & 0xf0) >> 4);
      buff2[2] = ((buff1[1] & 0x0f) << 2) + ((buff1[2] & 0xc0) >> 6);
      buff2[3] = buff1[2] & 0x3f;

      for (uint8_t j = 0;  j < (i+1);  j++) {
	if (file_ != NULL) {
	  fputc(b64encodeTable[(uint8_t)buff2[j]], file_);
	}
	else {
	  alt_ << b64encodeTable[(uint8_t)buff2[j]];
	}
      }

      while (i++ < 3) {
	if (file_ != NULL) {
	  fputc('=', file_);
	}
	else {
	  alt_ << "=";
	}
      }

    }
  }
};

class MyInputStream : public avro::InputStream {
public:
  FILE *file_;
  std::stringstream alt_;

  const size_t chunkSize_;
  uint8_t *rawdata_;
  uint8_t *data_;
  size_t byteCount_;

  char buff2[4];
  uint8_t i;
  bool endProcessed_;

  clock_t last_heartbeat;

  MyInputStream(FILE *f, size_t chunkSize): file_(f), chunkSize_(chunkSize), byteCount_(0), i(0), endProcessed_(false), last_heartbeat(0) {
    rawdata_ = new uint8_t[chunkSize_];
    data_ = new uint8_t[chunkSize_];
  }

  ~MyInputStream() {
    delete [] rawdata_;
    delete [] data_;
  }

  bool next(const uint8_t **data, size_t *len) {
    clock_t now = clock();
    if (now - last_heartbeat > 10 * CLOCKS_PER_SEC) {
      std::cerr << "GeoPictureSerializer::MyInputStream clock() = " << now << ", seconds since last_heartbeat = " << 1.0*(now - last_heartbeat)/CLOCKS_PER_SEC << std::endl;
      std::cerr << "reporter:status:GeoPictureSerializer::MyInputStream clock() = " << now << ", seconds since last_heartbeat = " << 1.0*(now - last_heartbeat)/CLOCKS_PER_SEC << std::endl;
      last_heartbeat = now;
    }

    int valid = process();
    if (valid == 0) { return false; }

    *data = data_;
    *len = valid;
    byteCount_ += valid;

    return true;
  }

  void backup(size_t len) {
    fseek(file_, -len, SEEK_CUR);
    byteCount_ -= len;
  }

  void skip(size_t len) {
    fseek(file_, len, SEEK_CUR);
    byteCount_ += len;
  }

  size_t byteCount() const {
    return byteCount_;
  }

  size_t process() {
    int valid;
    if (file_ != NULL) {
      valid = fread(rawdata_, sizeof(uint8_t), chunkSize_, file_);
    }
    else {
      valid = alt_.readsome((char *)rawdata_, chunkSize_);
    }
    if (valid == 0) { return endProcess(0); }

    int l = 0;
    for (int k = 0;  k < valid;  k++) {
      buff2[i] = rawdata_[k];
      if (buff2[i] == '=') { return endProcess(k); }

      if (++i == 4) {
	for (i = 0;  i != 4;  i++) { buff2[i] = b64decodeTable[(uint8_t)buff2[i]]; }

	data_[l++] = (char)((buff2[0] << 2) + ((buff2[1] & 0x30) >> 4));
	data_[l++] = (char)(((buff2[1] & 0xf) << 4) + ((buff2[2] & 0x3c) >> 2));
	data_[l++] = (char)(((buff2[2] & 0x3) << 6) + buff2[3]);

	i = 0;
      }
    }

    return l;
  }

  size_t endProcess(int l) {
    if (endProcessed_) { return l; }

    if (i) {
      for (uint8_t j = i;  j < 4;  j++) { buff2[j] = '\0'; }
      for (uint8_t j = 0;  j < 4;  j++) { buff2[j] = b64decodeTable[(uint8_t)buff2[j]]; }
      
      char buff1[4];
      buff1[0] = (buff2[0] << 2) + ((buff2[1] & 0x30) >> 4);
      buff1[1] = ((buff2[1] & 0xf) << 4) + ((buff2[2] & 0x3c) >> 2);
      buff1[2] = ((buff2[2] & 0x3) << 6) + buff2[3];
      
      for (uint8_t j = 0;  j < (i-1);  j++) { data_[l++] = (char)buff1[j]; }
    }

    endProcessed_ = true;
    return l;
  }
};

static PyObject *GeoPictureSerializer_GeoPicture_serialize(GeoPictureSerializer_GeoPicture *self, PyObject *args) {
  PyObject *pyfile = NULL;
  FILE *file = NULL;
  if (!PyArg_ParseTuple(args, "|O", &pyfile)) {
    PyErr_SetString(PyExc_TypeError, "input a file object for writing or nothing (to get a string back)");
    return NULL;
  }
  if (pyfile != NULL) {
    if (!PyFile_Check(pyfile)) {
      PyErr_SetString(PyExc_TypeError, "argument must be a file object");
      return NULL;
    }
    file = PyFile_AsFile(pyfile);
  }

  if (!PyDict_Check(self->metadata)) {
    PyErr_SetString(PyExc_TypeError, "metadata must be a dictionary");
    return NULL;
  }
  if (!PySequence_Check(self->bands)) {
    PyErr_SetString(PyExc_TypeError, "bands must be a sequence");
    return NULL;
  }
  if (!PyArray_Check(self->picture)) {
    PyErr_SetString(PyExc_TypeError, "picture must be a NumPy array");
    return NULL;
  }
  if (PyArray_NDIM(self->picture) != 3  ||  PyArray_SIZE(self->picture) == 0) {
    PyErr_SetString(PyExc_TypeError, "picture must be a three-dimensional NumPy array (height, width, and spectral bands)");
    return NULL;
  }

  gpwm::GeoPictureWithMetadata p;

  PyObject *key, *value;
  Py_ssize_t pos = 0;
  while (PyDict_Next(self->metadata, &pos, &key, &value)) {
    if (!PyString_Check(key)) {
      PyErr_SetString(PyExc_TypeError, "metadata dictionary keys must be strings");
      return NULL;
    }
    if (!PyString_Check(value)) {
      PyErr_SetString(PyExc_TypeError, "metadata dictionary values must be strings");
      return NULL;
    }
    p.metadata[std::string(PyString_AsString(key))] = std::string(PyString_AsString(value));
  }

  for (Py_ssize_t i = 0;  i < PySequence_Size(self->bands);  i++) {
    PyObject *o = PySequence_GetItem(self->bands, i);
    if (!PyString_Check(o)) {
      Py_DECREF(o);
      PyErr_SetString(PyExc_TypeError, "bands must be a sequence of strings");
      return NULL;
    }
    p.bands.push_back(std::string(PyString_AsString(o)));
    Py_DECREF(o);
  }

  npy_intp *dims = PyArray_DIMS(self->picture);
  p.height = dims[0];
  p.width = dims[1];
  p.depth = dims[2];
  p.dtype = PyArray_TYPE(self->picture);
  p.itemsize = PyArray_ITEMSIZE(self->picture);
  p.nbytes = PyArray_NBYTES(self->picture);
  p.fortran = PyArray_ISFORTRAN(self->picture);
  switch (PyArray_DESCR(self->picture)->byteorder) {
    case '<':
      p.byteorder = gpwm::LittleEndian;
      break;
    case '>':
      p.byteorder = gpwm::BigEndian;
      break;
    case '=':
      p.byteorder = gpwm::NativeEndian;
      break;
    default:
      p.byteorder = gpwm::IgnoreEndian;
      break;
  }
  
  if (p.depth != PySequence_Size(self->bands)) {
    PyErr_SetString(PyExc_TypeError, "the number of bands must be equal to the depth of picture");
    return NULL;
  }

  PyArray_NonzeroFunc *nonzero = PyArray_DESCR(self->picture)->f->nonzero;

  PyArrayObject *picture = (PyArrayObject*)(self->picture);
  NpyIter *iter = NpyIter_New(picture, NPY_ITER_READONLY | NPY_ITER_EXTERNAL_LOOP, (p.fortran ? NPY_FORTRANORDER : NPY_CORDER), NPY_NO_CASTING, NULL);
  if (iter == NULL) {
    return NULL;
  }

  NpyIter_IterNextFunc *iternext = NpyIter_GetIterNext(iter, NULL);
  if (iternext == NULL) {
    NpyIter_Deallocate(iter);
    return NULL;
  }

  char **dataptr = NpyIter_GetDataPtrArray(iter);
  npy_intp *strideptr = NpyIter_GetInnerStrideArray(iter);
  npy_intp *innersizeptr = NpyIter_GetInnerLoopSizePtr(iter);
  
  int64_t index = 0;
  gpwm::ZeroSuppressed *current = NULL;
  do {
    char *data = *dataptr;
    npy_intp stride = *strideptr;
    npy_intp count = *innersizeptr;

    while (count--) {
      if (nonzero(data, picture)) {
	if (!current) {
	  p.data.push_back(gpwm::ZeroSuppressed());
	  current = &p.data.back();
	  current->index = index;
	}
	for (int i = 0;  i < p.itemsize;  i++) {
	  current->strip.push_back(data[i]);
	}
      }
      else {
	current = NULL;
      }

      data += stride;
      index++;
    }
  } while (iternext(iter));

  NpyIter_Deallocate(iter);

  if (file != NULL) { PyFile_IncUseCount((PyFileObject*)pyfile); }

  std::auto_ptr<MyOutputStream> out = std::auto_ptr<MyOutputStream>(new MyOutputStream(file, 4*1024));
  try {
    avro::EncoderPtr e = avro::validatingEncoder(self->validSchema, avro::binaryEncoder());
    e->init(*out);
    avro::encode(*e, p);
    out->flush();
    out->finishUp();
  }
  catch (avro::Exception) {
    PyErr_SetString(PyExc_IOError, "Avro could not serialize this GeoPicture");
    return NULL;
  }

  if (file != NULL) { PyFile_DecUseCount((PyFileObject*)pyfile); }

  if (file != NULL) {
    return Py_BuildValue("O", Py_None);
  }
  else {
    return PyString_FromString(out->alt_.str().c_str());
  }
}

static PyObject *GeoPictureSerializer_deserialize(PyObject *self, PyObject *args) {
  PyObject *pyfile = NULL;
  FILE *file = NULL;
  if (!PyArg_ParseTuple(args, "O", &pyfile)) {
    PyErr_SetString(PyExc_TypeError, "pass a file object for reading or a string");
    return NULL;
  }

  std::auto_ptr<MyInputStream> in;

  if (PyFile_Check(pyfile)) {
    file = PyFile_AsFile(pyfile);
    in = std::auto_ptr<MyInputStream>(new MyInputStream(file, 4*1024));
  }
  else if (PyString_Check(pyfile)) {
    in = std::auto_ptr<MyInputStream>(new MyInputStream(file, 4*1024));
    in->alt_ << PyString_AsString(pyfile);
  }
  else {
    PyErr_SetString(PyExc_TypeError, "argument must be a file object or a string");
    return NULL;
  }

  PyObject *output = PyObject_CallObject((PyObject*)(&GeoPictureSerializer_GeoPictureType), NULL);
  GeoPictureSerializer_GeoPicture *coutput = (GeoPictureSerializer_GeoPicture*)output;

  gpwm::GeoPictureWithMetadata p;
  try {
    avro::DecoderPtr d = avro::validatingDecoder(coutput->validSchema, avro::binaryDecoder());
    d->init(*in);

    avro::decode(*d, p);
  }
  catch (avro::Exception) {
    PyErr_SetString(PyExc_IOError, "Avro could not deserialize this string");
    return NULL;
  }

  for (std::map<std::string,std::string>::const_iterator iter = p.metadata.begin();  iter != p.metadata.end();  ++iter) {
    PyObject *value = PyString_FromString(iter->second.c_str());

    if (PyDict_SetItemString(coutput->metadata, iter->first.c_str(), value) != 0) {
      Py_DECREF(value);
      Py_DECREF(output);
      return NULL;
    }
    Py_DECREF(value);
  }

  for (std::vector<std::string>::const_iterator iter = p.bands.begin();  iter != p.bands.end();  ++iter) {
    PyObject *value = PyString_FromString(iter->c_str());
    if (PyList_Append(coutput->bands, value) != 0) {
      Py_DECREF(value);
      Py_DECREF(output);
      return NULL;
    }
    Py_DECREF(value);
  }

  Py_DECREF(coutput->picture);
  npy_intp dims[3] = {p.height, p.width, p.depth};

  coutput->picture = PyArray_EMPTY(3, dims, p.dtype, p.fortran);

  if (coutput->picture == NULL) {
    Py_DECREF(output);
    PyErr_Format(PyExc_MemoryError, "could not allocate a %dx%dx%d array", p.height, p.width, p.depth);
    return NULL;
  }

  int oldbyteorder, newbyteorder;
  switch (p.byteorder) {
    case gpwm::LittleEndian:
      oldbyteorder = NPY_LITTLE;
      break;
    case gpwm::BigEndian:
      oldbyteorder = NPY_BIG;
      break;
    case gpwm::NativeEndian:
      oldbyteorder = NPY_NATIVE;
      break;
    case gpwm::IgnoreEndian:
      oldbyteorder = NPY_IGNORE;
      break;
  }
  switch (PyArray_DESCR(coutput->picture)->byteorder) {
    case '<':
      newbyteorder = NPY_LITTLE;
      break;
    case '>':
      newbyteorder = NPY_BIG;
      break;
    case '=':
      newbyteorder = NPY_NATIVE;
      break;
    default:
      newbyteorder = NPY_IGNORE;
      break;
  }

  bool swapbytes = !PyArray_EquivByteorders(oldbyteorder, newbyteorder);

  PyArrayObject *picture = (PyArrayObject*)(coutput->picture);
  NpyIter *iter = NpyIter_New(picture, NPY_ITER_WRITEONLY | NPY_ITER_EXTERNAL_LOOP, (p.fortran ? NPY_FORTRANORDER : NPY_CORDER), NPY_NO_CASTING, NULL);
  if (iter == NULL) {
    Py_DECREF(output);
    return NULL;
  }

  NpyIter_IterNextFunc *iternext = NpyIter_GetIterNext(iter, NULL);
  if (iternext == NULL) {
    NpyIter_Deallocate(iter);
    Py_DECREF(output);
    return NULL;
  }

  char **dataptr = NpyIter_GetDataPtrArray(iter);
  npy_intp *strideptr = NpyIter_GetInnerStrideArray(iter);
  npy_intp *innersizeptr = NpyIter_GetInnerLoopSizePtr(iter);
  
  std::vector<gpwm::ZeroSuppressed>::const_iterator zs = p.data.begin();
  std::vector<gpwm::ZeroSuppressed>::const_iterator p_data_end = p.data.end();
  std::vector<uint8_t>::const_iterator byte;
  std::vector<uint8_t>::const_iterator zs_strip_end;
  if (zs != p_data_end) {
    byte = zs->strip.begin();
    zs_strip_end = zs->strip.end();
  }

  bool filling = false;

  int64_t index = 0;
  do {
    char *data = *dataptr;
    npy_intp stride = *strideptr;
    npy_intp count = *innersizeptr;

    while (count--) {
      if (zs != p_data_end  &&  zs->index == index) { filling = true; }

      if (filling) {
	if (swapbytes) {
	  for (int i = p.itemsize - 1;  i >= 0;  i--) { data[i] = *byte; ++byte; }
	}
	else {
	  for (int i = 0;  i < p.itemsize;  i++) { data[i] = *byte; ++byte; }
	}

	if (byte == zs_strip_end) {
	  ++zs;
	  if (zs != p_data_end) {
	    byte = zs->strip.begin();
	    zs_strip_end = zs->strip.end();
	  }
	  filling = false;
	}

      }

      else {
	for (int i = 0;  i < p.itemsize;  i++) { data[i] = 0; }
      }

      data += stride;
      index++;
    }
  } while (iternext(iter));

  NpyIter_Deallocate(iter);
  return output;
}

static PyObject *GeoPictureSerializer_GeoPicture_getPicture(GeoPictureSerializer_GeoPicture *self, void *closure) {
  return Py_BuildValue("O", self->picture);
}

static PyObject *GeoPictureSerializer_GeoPicture_setPicture(GeoPictureSerializer_GeoPicture *self, PyObject *value, void *closure) {
  if (value == NULL) {
    Py_DECREF(self->picture);
    self->picture = Py_BuildValue("O", Py_None);
  }
  else {
    Py_DECREF(self->picture);
    Py_INCREF(value);
    self->picture = value;
  }
  return 0;
}

static PyMethodDef GeoPictureSerializer_methods[] = {
   {"deserialize", (PyCFunction)(GeoPictureSerializer_deserialize), METH_VARARGS, "Deserialize a GeoPicture from a string."},

  {NULL}
};

static PyObject *moduleinit(void) {
  PyObject *m;

  GeoPictureSerializer_GeoPictureType.tp_new = PyType_GenericNew;
  if (PyType_Ready(&GeoPictureSerializer_GeoPictureType) < 0) return NULL;

  MOD_DEF(m, "GeoPictureSerializer", "Module to serialize geo-tagged images and their metadata with Avro", GeoPictureSerializer_methods);
  if (m == NULL) {
    return NULL;
  }
  Py_INCREF(&GeoPictureSerializer_GeoPictureType);
  PyModule_AddObject(m, "GeoPicture", (PyObject*)(&GeoPictureSerializer_GeoPictureType));

  import_array1(m);
}

#if PY_MAJOR_VERSION < 3
    PyMODINIT_FUNC initGeoPictureSerializer(void)
    {
        moduleinit();
    }
#else
    PyMODINIT_FUNC PyInit_GeoPictureSerializer(void)
    {
        return moduleinit();
    }
#endif
