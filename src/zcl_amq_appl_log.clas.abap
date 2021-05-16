* See https://github.com/se38/abapMQ_deamon
********************************************************************************
* The MIT License (MIT)
*
* Copyright (c) 2021 Uwe Fetzer and the abapMQ Daemons Contributors
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
********************************************************************************
"! <p class="shorttext synchronized" lang="en">abapMQ Daemons Applicatio Log</p>
CLASS zcl_amq_appl_log DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    "! <p class="shorttext synchronized" lang="en">Add new message to an existing or new Log Handle</p>
    "! @parameter i_log_handle | <p class="shorttext synchronized" lang="en">Log Handle, if not new</p>
    "! @parameter i_object | <p class="shorttext synchronized" lang="en">Application Log Object</p>
    "! @parameter i_subobject | <p class="shorttext synchronized" lang="en">Application Log Subobject</p>
    "! @parameter i_days | <p class="shorttext synchronized" lang="en">Days until expiration</p>
    "! @parameter i_message_type | <p class="shorttext synchronized" lang="en">Message type</p>
    "! @parameter i_text | <p class="shorttext synchronized" lang="en">Free text</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Log Handle</p>
    METHODS add_message
      IMPORTING i_log_handle    TYPE balloghndl OPTIONAL
                i_object        TYPE balobj_d DEFAULT 'APPL_LOG'
                i_subobject     TYPE balsubobj DEFAULT 'OTHERS'
                i_days          TYPE i DEFAULT 30
                i_message_type  TYPE symsgty DEFAULT 'S'
                i_text          TYPE string
      RETURNING VALUE(r_result) TYPE balloghndl.

  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA log_handle TYPE balloghndl.

    METHODS create_new_log
      IMPORTING i_object        TYPE balobj_d
                i_subobject     TYPE balsubobj
                i_days          TYPE i
      RETURNING VALUE(r_result) TYPE balloghndl.
ENDCLASS.



CLASS zcl_amq_appl_log IMPLEMENTATION.
  METHOD add_message.

    IF i_log_handle IS INITIAL
    AND log_handle IS INITIAL.
      log_handle = r_result = create_new_log(
                                i_object = i_object
                                i_subobject = i_subobject
                                i_days = i_days ).
    ELSE.
      IF i_log_handle IS INITIAL.
        r_result = log_handle.
      ELSE.
        r_result = log_handle = i_log_handle.
      ENDIF.
    ENDIF.

    CALL FUNCTION 'BAL_LOG_MSG_ADD_FREE_TEXT'
      EXPORTING
        i_log_handle     = log_handle
        i_msgty          = i_message_type
        i_text           = CONV bapi_msg( i_text )
      EXCEPTIONS
        log_not_found    = 1
        msg_inconsistent = 2
        log_is_full      = 3
        OTHERS           = 4.

    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    CALL FUNCTION 'BAL_DB_SAVE'
      EXPORTING
        i_save_all = abap_true
      EXCEPTIONS
        OTHERS     = 0.

  ENDMETHOD.

  METHOD create_new_log.

    CALL FUNCTION 'BAL_LOG_CREATE'
      EXPORTING
        i_s_log      = VALUE bal_s_log(
                         object = 'APPL_LOG'
                         subobject = 'OTHERS'
                         aldate_del = sy-datum + i_days )
      IMPORTING
        e_log_handle = r_result
      EXCEPTIONS
        OTHERS       = 0.

  ENDMETHOD.

ENDCLASS.
