#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <map>
#include <ctime>
#include <iostream>

#include "SlsCoding.h"
#include "SlsLogPbParser.h"
#include "log_builder.h"

using namespace std;

bool get_str_bytes_from_pyobj(PyObject * pyObj ,char ** val, Py_ssize_t * valLen, bool allowBytes) {
    if(PyUnicode_CheckExact(pyObj)){
        *val = (char *) PyUnicode_AsUTF8AndSize(pyObj, valLen);
    }else if(allowBytes&&PyBytes_CheckExact(pyObj)){
        *val = (char *) PyBytes_AsString(pyObj);
        *valLen = PyBytes_Size(pyObj);
    }else {
        return false;
    }
    if(*val != NULL) {
        return true;
    }
    return false;
}

PyObject * get_pyobj_from_str_bytes(const char * val, int valLen) {
      PyObject *pyObj = PyUnicode_FromStringAndSize(val, valLen);
      if(NULL==pyObj){
          pyObj = PyBytes_FromStringAndSize(val, valLen);
          PyErr_Clear();
      }
      return pyObj;
}

bool check_tuple_and_size(PyObject * obj, int exceptSize) {
    if(!PyTuple_CheckExact(obj)){
        return false;
    }
    if(PyTuple_Size(obj)!=exceptSize){
       return false;
    }
    return true;
}

bool check_list_and_size(PyObject * obj, int exceptSize) {
    if(!PyList_CheckExact(obj)){
        return false;
    }
    if(PyList_Size(obj)!=exceptSize){
       return false;
    }
    return true;
}


string get_obj_err_msg(string err, PyObject * obj) {
    PyObject * pyErrInfo = PyObject_Repr(obj);
    err += PyUnicode_AsUTF8(pyErrInfo);
    Py_XDECREF(pyErrInfo);
    return err;
}

string to_math_string(int index){
    if (index+1 == 6){
        return "sixth";
    }else if (index+1 == 7){
        return "seventh";
    }
    return "unknown";
}

void value_check_msg(PyObject * obj, PyObject * pyWarnMsgList, string err) {
    PyErr_Clear();
    err = get_obj_err_msg(err, obj);

    PyObject * pyWarnMsg = PyUnicode_FromString(err.c_str());
    PyList_Append(pyWarnMsgList, pyWarnMsg);
    Py_XDECREF(pyWarnMsg);
}

void add_warning_msg(PyObject * pyWarnMsgList, string err){
    PyObject * pyWarnMsg = PyUnicode_FromString(err.c_str());
    PyList_Append(pyWarnMsgList, pyWarnMsg);
    Py_XDECREF(pyWarnMsg);
}

static PyObject * write_pb(PyObject * self, PyObject * args) {
    bool has_nano = false;
    int index_content = 5;
    if (check_list_and_size(args, 6)){
    }else if(check_list_and_size(args, 7)){
       has_nano = true;
       index_content = 6;
    }
    else{
       string err = get_obj_err_msg("slspb error: write_pb except a list,example [compress_flag,topic_str, source_str,tag_list, time_list, time_nano_part(option), pyContentKeyv_list], got: ",
                                       args);
       PyErr_SetString(PyExc_Exception, err.c_str());
       return NULL;
    }

    long logtime;
    long logtime_nano_part;
    long skipCnt = 0;
    long totalLogCnt = 0;
    long serializedLogCnt = 0;
    long serializedLogKVCnt = 0;
    char * key;
    char * val;
    Py_ssize_t keyLen, valLen;
    long i,j,n;
    long rawSize = 0;
    PyObject * pyWarnMsgList = PyList_New(0);

    int lzoCompress = (int) PyLong_AsLong(PyList_GetItem(args, 0));
    if(PyErr_Occurred()){
       string err = get_obj_err_msg("slspb error: except int at fisrt element in list, got: ", PyList_GetItem(args, 0));
       PyErr_SetString(PyExc_Exception, err.c_str());
       return NULL;
    }

    if(lzoCompress!=0&&lzoCompress!=1){
       string err = get_obj_err_msg("slspb error: except 0 or 1 at fisrt element in list, got: ", PyList_GetItem(args, 0));
       PyErr_SetString(PyExc_Exception, err.c_str());
       return NULL;
    }

    log_group_builder* builder = log_group_create();
    PyObject * pyTopic = PyList_GetItem(args, 1);

    Py_ssize_t sourceLen, topicLen;
    char * source;
    char * topic;
    string emptyStr = "";

    if(Py_None!=pyTopic){
        if(!get_str_bytes_from_pyobj(pyTopic, &topic, &topicLen, true)){
           string err = get_obj_err_msg("slspb error: except string at second element(topic) in list, got: ", pyTopic);
           PyErr_SetString(PyExc_Exception, err.c_str());
           return NULL;
        }

        if(topicLen > 255){
            topicLen = 255;
            add_warning_msg(pyWarnMsgList, "topic is too long, limit to 255");
        }
        if (topicLen!=0){
            add_topic(builder, topic, topicLen);
        } else {
            add_topic(builder, "", 0);
        }
    } else {
        add_topic(builder, "", 0);
    }

    PyObject * pySource = PyList_GetItem(args, 2);
    if(Py_None!=pySource){
        if(!get_str_bytes_from_pyobj(pySource, &source, &sourceLen, true)){
           string err = get_obj_err_msg("slspb error: except string at third element(source) in list, got: ", pySource);
           PyErr_SetString(PyExc_Exception, err.c_str());
           return NULL;
        }

        if (sourceLen > 128){
            sourceLen = 128;
            add_warning_msg(pyWarnMsgList, "source is too long, limit to 128");
        }
        if (sourceLen!=0){
            add_source(builder, source, sourceLen);
        }else{
            add_source(builder, "127.0.0.1", strlen("127.0.0.1"));
        }
    } else{
        add_source(builder, "127.0.0.1", strlen("127.0.0.1"));
    }

    n = PyList_Size(PyList_GetItem(args, 3));
    if(PyErr_Occurred()){
        string err = get_obj_err_msg("slspb error: except tag list at fourth element in list, like [(tagk1,tagv1),(tagk2,tagv2)], got: ", PyList_GetItem(args, 3));
        PyErr_SetString(PyExc_Exception, err.c_str());
        return NULL;
    }

    for (i=0; i<n; i++) {
        PyObject * pyTagTuple = PyList_GetItem(PyList_GetItem(args, 3),i);
        if(!check_tuple_and_size(pyTagTuple, 2)){
            string err = get_obj_err_msg("slspb error: except tag list at fourth element in list, like [(tagk1,tagv1),(tagk2,tagv2)], tag key and value should be string, got: ",
                             pyTagTuple);
            PyErr_SetString(PyExc_Exception, err.c_str());
            return NULL;
        }

        PyObject * pyTagKey = PyTuple_GET_ITEM(pyTagTuple,0);
        if(!get_str_bytes_from_pyobj(pyTagKey, &key, &keyLen, false)){
           string err = get_obj_err_msg("slspb error: except tag list at fourth element in list, like [(tagk1,tagv1),(tagk2,tagv2)], tag key should be string, got: ",
                             pyTagKey);
           PyErr_SetString(PyExc_Exception, err.c_str());
           return NULL;
        }

        PyObject * pyTagVal = PyTuple_GET_ITEM(pyTagTuple,1);
        if(!get_str_bytes_from_pyobj(pyTagVal, &val, &valLen, true)){
           if(PyErr_Occurred()){
               string err = "slspb warning: bad log tag skipped, got: ";
               value_check_msg(pyTagVal, pyWarnMsgList, err);
               continue;
           } else if(Py_None==pyTagVal){
               val = (char *) emptyStr.c_str();
               valLen = 0;
           } else {
               string err = get_obj_err_msg("slspb error: except tag list at fourth element in list, like [(tagk1,tagv1),(tagk2,tagv2)], tag value should be string or bytes, got: ",
                                 pyTagVal);
               PyErr_SetString(PyExc_Exception, err.c_str());
               return NULL;
           }
        }
        if(valLen > 255){
            valLen = 255;
            add_warning_msg(pyWarnMsgList, "tag value is too long, limit to 255");
        }
        add_tag(builder, key, keyLen, val, valLen);
    }

    long tsCnt = PyList_Size(PyList_GetItem(args, 4));
    if(PyErr_Occurred()){
       string err = get_obj_err_msg("slspb error: except time list at fifth element in list, like [ts1, ts2..], ts should be int, got: ",
                       PyList_GetItem(args, 4));
       PyErr_SetString(PyExc_Exception, err.c_str());
       return NULL;
    }
    if (has_nano){
        long tnsCnt = PyList_Size(PyList_GetItem(args, 5));
        if(PyErr_Occurred()){
            string err = get_obj_err_msg("slspb error: except time_ns_part list at sixth element in list, like [tns1, tns2..], ts should be int, got: ",
                            PyList_GetItem(args, 5));
            PyErr_SetString(PyExc_Exception, err.c_str());
            return NULL;
        }
    }
    totalLogCnt = PyList_Size(PyList_GetItem(args, index_content));
    if(PyErr_Occurred()){
        string err = get_obj_err_msg("slspb error: except content list at " + to_math_string(index_content) + " element in list, like [[(key1,value1),..],], content key and value should be string, got: ",
                          PyList_GetItem(args, index_content));
        PyErr_SetString(PyExc_Exception, err.c_str());
        return NULL;
    }

    if (tsCnt != totalLogCnt) {
        string err = "slspb error: the count of ts and logs mismath, got:  ts count: " + to_string(tsCnt) + " log count:" + to_string(totalLogCnt);
        PyErr_SetString(PyExc_TypeError, err.c_str());
        return NULL;
    }

    if (tsCnt == 0) {
        PyErr_SetString(PyExc_TypeError, "slspb error: the count of ts and logs should large than 1");
        return NULL;
    }
    for (i=0; i<totalLogCnt; i++) {
        PyObject * pyLogTime = PyList_GetItem(PyList_GetItem(args, 4),i);
        if(!PyLong_CheckExact(pyLogTime)){
           string err = get_obj_err_msg("slspb error: except time list at fifth element in list, like [ts1, ts2..], ts should be int, got: ",
                                 PyList_GetItem(args, 4));
           PyErr_SetString(PyExc_Exception, err.c_str());
           return NULL;
        }

        logtime = PyLong_AsLong(pyLogTime);
        if(logtime<268435457||logtime>4813906244){
           //ts should not less than 268435457 or large than 102 year later
           value_check_msg(pyLogTime, pyWarnMsgList, "slspb error: ts is value invalid, will be reset to system time, got: ");
           logtime = (long) std::time(0); // reset to system time
        }
        
        add_log_begin(builder);
        add_log_time(builder, (u_int32_t)logtime);
        PyObject * pyContentList;
        if (has_nano){
            PyObject * pyLogTimeNano = PyList_GetItem(PyList_GetItem(args, 5),i);
            if(!PyLong_CheckExact(pyLogTimeNano)){
                string err = get_obj_err_msg("slspb error: except time_ns_part list at sixth element in list, like [tns1, tns2..], tns should be int, got: ",
                                        PyList_GetItem(args, 5));
                PyErr_SetString(PyExc_Exception, err.c_str());
                return NULL;
            }
            logtime_nano_part = PyLong_AsLong(pyLogTimeNano);
            if(logtime_nano_part>=0 && logtime_nano_part<std::nano::den){
                //only tns part is larger than 0 and less than 1000000000, time_ns_part can be added
                add_log_timenano_part(builder, (u_int32_t)logtime_nano_part);
            }
        } 
        pyContentList = PyList_GetItem(PyList_GetItem(args, index_content), i);
        n = PyList_Size(pyContentList);
        if(PyErr_Occurred()){
            string err = get_obj_err_msg("slspb error: except content list at " + to_math_string(index_content) + " element in list, like [[(key1,value1),..],], content key and value should be string, got: ",PyList_GetItem(args, index_content));
            PyErr_SetString(PyExc_Exception, err.c_str());
            return NULL;
        }

        serializedLogKVCnt = 0;
        for (j=0; j<n; j++){
            PyObject * pyContentTuple = PyList_GetItem(pyContentList,j);
            if(!check_tuple_and_size(pyContentTuple, 2)){
                string err = get_obj_err_msg("slspb error: except content list at " + to_math_string(index_content) + " element in list, like [[(key1,value1),..],], content key and value should be string, got: ",pyContentTuple);
                PyErr_SetString(PyExc_Exception, err.c_str());
                return NULL;
            }

            PyObject * pyContentKey = PyTuple_GET_ITEM(pyContentTuple,0);
            if(!get_str_bytes_from_pyobj(pyContentKey, &key, &keyLen, false)){
               string err = get_obj_err_msg("slspb skip key warning: except content key be string, got: ", pyContentKey);
               add_warning_msg(pyWarnMsgList, err);
               PyErr_Clear();
               continue;
            }

            PyObject * pyContentVal = PyTuple_GET_ITEM(pyContentTuple,1);
            if(!get_str_bytes_from_pyobj(pyContentVal, &val, &valLen, true)){
                if(PyErr_Occurred()){
                    string err = get_obj_err_msg("slspb warning: bad log content value skipped, content key: ", pyContentKey);
                    err += " value: ";
                    err = get_obj_err_msg(err, pyContentVal);
                    add_warning_msg(pyWarnMsgList, err);
                    PyErr_Clear();
                    continue;
                }  else if(Py_None==pyContentVal){
                    val = (char *) emptyStr.c_str();
                    valLen = 0;
                } else {
                    string err;
                    err = get_obj_err_msg("slspb error: except content list at "+ to_math_string(index_content) +" element in list, like [[(key1,value1),..],], content value should be string or bytes , got: ", pyContentVal);
                    PyErr_SetString(PyExc_Exception, err.c_str());
                    return NULL;
                }
            }
            serializedLogKVCnt++;
            add_log_key_value(builder, key, keyLen, val, valLen);
        }
        if(serializedLogKVCnt != 0){
            serializedLogCnt ++;
        }
        add_log_end(builder);
    }

    lz4_log_buf* logBuf ;

    if (lzoCompress == 0){
        logBuf = serialize_to_proto_buf_with_malloc_no_lz4(builder);
    } else {
        logBuf = serialize_to_proto_buf_with_malloc_lz4(builder);
    }
    if(NULL==logBuf){
        PyErr_SetString(PyExc_Exception, "slspb error: serialize data failed!");
        return NULL;
    }
    PyObject * pbBuf = PyBytes_FromStringAndSize((char *) logBuf->data, (Py_ssize_t) logBuf->length);
    rawSize = (long) logBuf->raw_length;

    log_group_destroy(builder);
    free_lz4_log_buf(logBuf);

    skipCnt = totalLogCnt - serializedLogCnt;
    PyObject *pyRet = PyTuple_New(4);
    PyTuple_SetItem(pyRet, 0, pbBuf);
    PyTuple_SetItem(pyRet, 1, PyLong_FromLong(rawSize));
    PyTuple_SetItem(pyRet, 2, PyLong_FromLong(skipCnt));
    PyTuple_SetItem(pyRet, 3, pyWarnMsgList);

    return pyRet;
}


static PyObject * parse_pb(PyObject * self, PyObject * args) {
   if(!check_list_and_size(args, 2)) {
      PyErr_SetString(PyExc_Exception, "slspb error: parse_pb except a list, example [pb, timeAsStr]");
      return NULL;
   }

   const char * buffer = (char*)PyBytes_AsString(PyList_GetItem(args, 0));
   if(NULL==buffer){
     string err = get_obj_err_msg("slspb error: except bytes at first element in list, got: ", PyList_GetItem(args, 0));
     PyErr_SetString(PyExc_Exception, err.c_str());
     return NULL;
   }
   int length = (int)PyBytes_Size(PyList_GetItem(args, 0));
   if(PyErr_Occurred()){
     string err = get_obj_err_msg("slspb error: get length of pb bytes failed, first element value is ",  PyList_GetItem(args, 0));
     PyErr_SetString(PyExc_Exception, err.c_str());
     return NULL;
   }

   int timeAsStr = (int)PyLong_AsLong(PyList_GetItem(args, 1));
   if(PyErr_Occurred()){
     string err = get_obj_err_msg("slspb error: except int at second element in list, got: ", PyList_GetItem(args, 1));
     PyErr_SetString(PyExc_Exception, err.c_str());
     return NULL;
   }
   if(timeAsStr!=0&&timeAsStr!=1) {
     string err = get_obj_err_msg("slspb error: except second element value 0 or 1, got: ", PyList_GetItem(args, 1));
     PyErr_SetString(PyExc_Exception, err.c_str());
     return NULL;
   }

   apsara::sls::SlsLogGroupListFlatPb loggroupList(buffer, length) ;
   loggroupList.Decode();

   int loggroupCnt = loggroupList.GetLogGroupSize();

   PyObject *pyTopicKey = PyUnicode_FromString("__topic__");
   PyObject *pySourceKey = PyUnicode_FromString("__source__");
   PyObject *pyTimeKey = PyUnicode_FromString("__time__");
   PyObject *pyTimeNanoKey = PyUnicode_FromString("__time_ns_part__");

   int tagCount;
   int logCount;

   PyObject *pyLogList = PyList_New(0);
   for(int i=0; i<loggroupCnt; i++) {
       const apsara::sls::SlsLogGroupFlatPb& flatLogGroup = loggroupList.GetLogGroupFlatPb(i);
       apsara::sls::SlsStringPiece tagKey;
       apsara::sls::SlsStringPiece tagValue;
       map<string , apsara::sls::SlsStringPiece> tagMap;
       tagCount = flatLogGroup.GetTagsSize();
       for (int tagIdx = 0; tagIdx < tagCount; ++ tagIdx) {
           const apsara::sls::SlsTagFlatPb & flatPb = flatLogGroup.GetTagFlatPb(tagIdx);
           if (flatPb.GetTagKeyValue(tagKey, tagValue)) {
               string tagKeyStr ;
               tagKeyStr.reserve(64);
               tagKeyStr.append("__tag__:");
               tagKeyStr.append(tagKey.mPtr, tagKey.mLen);
               tagMap.insert(pair<string , apsara::sls::SlsStringPiece>(tagKeyStr, tagValue));
           }
       }

       apsara::sls::SlsStringPiece topicStringPiece, sourceStringPiece;
       if (!flatLogGroup.GetTopic(topicStringPiece)) {
           topicStringPiece.mPtr = NULL;
           topicStringPiece.mLen = 0;
       }

       if (!flatLogGroup.GetSource(sourceStringPiece)){
           sourceStringPiece.mPtr = NULL;
           sourceStringPiece.mLen = 0;
       }

       logCount = flatLogGroup.GetLogsSize();

       for (int logIdx = 0; logIdx < logCount; ++logIdx) {
          PyObject *pyLogDict = PyDict_New();
          PyObject *pyTopicVal = get_pyobj_from_str_bytes(topicStringPiece.mPtr,topicStringPiece.mLen);

          PyDict_SetItem(pyLogDict, pyTopicKey, pyTopicVal);
          Py_XDECREF(pyTopicVal);

          PyObject *pySourceVal = get_pyobj_from_str_bytes(sourceStringPiece.mPtr, sourceStringPiece.mLen);
          PyDict_SetItem(pyLogDict, pySourceKey, pySourceVal);
          Py_XDECREF(pySourceVal);

          const apsara::sls::SlsLogFlatPb & flatLog = flatLogGroup.GetLogFlatPb(logIdx);
          apsara::sls::SlsLogFlatPb::SlsLogFlatPbReader flatLogReader = flatLog.GetReader();

          PyObject *pyTimeVal ;
          if(timeAsStr == 0) {
              pyTimeVal = PyLong_FromLong(flatLog.GetTime());
          } else {
              pyTimeVal = PyUnicode_FromString(to_string(flatLog.GetTime()).c_str());
          }
          PyDict_SetItem(pyLogDict, pyTimeKey, pyTimeVal);;
          Py_XDECREF(pyTimeVal);

          if (flatLog.HasTimeNs()){
              PyObject *pyTimeNanoVal ;
              if(timeAsStr == 0) {
                  pyTimeNanoVal = PyLong_FromLong(flatLog.GetTimeNanoPart());
              } else {
                  pyTimeNanoVal = PyUnicode_FromString(to_string(flatLog.GetTimeNanoPart()).c_str());
              }
              PyDict_SetItem(pyLogDict, pyTimeNanoKey, pyTimeNanoVal);
              Py_XDECREF(pyTimeNanoVal);
          }

          apsara::sls::SlsStringPiece key, value;
          while (flatLogReader.GetNextKeyValue(key, value)) {
              PyObject *pyContentKey = get_pyobj_from_str_bytes(key.mPtr, key.mLen);
              PyObject *pyContentVal = get_pyobj_from_str_bytes(value.mPtr, value.mLen);

              PyDict_SetItem(pyLogDict, pyContentKey, pyContentVal);
              Py_XDECREF(pyContentKey);
              Py_XDECREF(pyContentVal);
          }
          for(auto &v : tagMap){
              PyObject *pyTagKey  = get_pyobj_from_str_bytes(v.first.c_str(), v.first.length());
              PyObject *pyTagVal  = get_pyobj_from_str_bytes(v.second.mPtr, v.second.mLen);

              PyDict_SetItem(pyLogDict, pyTagKey, pyTagVal);
              Py_XDECREF(pyTagKey);
              Py_XDECREF(pyTagVal);
          }
          PyList_Append(pyLogList, pyLogDict);
          Py_XDECREF(pyLogDict);
       }
   }
   Py_XDECREF(pyTopicKey);
   Py_XDECREF(pySourceKey);
   Py_XDECREF(pyTimeKey);
   Py_XDECREF(pyTimeNanoKey);

   PyObject *pyRet = PyTuple_New(2);
   PyTuple_SetItem(pyRet, 0, pyLogList);
   PyTuple_SetItem(pyRet, 1, PyLong_FromLong(loggroupCnt));
   return pyRet;
}


static PyMethodDef SLSPBMethods[] = {
    {"parse_pb",  parse_pb, METH_O, "parse_pb"},
    {"write_pb",  write_pb, METH_O, "write_pb"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef slspbmodule = {
    PyModuleDef_HEAD_INIT,
    "slspb",   /* name of module */
    "a module to parse and write sls pb", /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module,
                 or -1 if the module keeps state in global variables. */
    SLSPBMethods
};

PyMODINIT_FUNC PyInit_slspb(void) {
    PyObject *module = PyModule_Create(&slspbmodule);
    PyModule_AddIntConstant(module, "is_enable", 1);
    return module;
}
