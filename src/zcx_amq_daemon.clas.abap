* See https://github.com/se38/abapMQ_daemon
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
"! <p class="shorttext synchronized" lang="en">abapMQ Daemons Message Class</p>
CLASS zcx_amq_daemon DEFINITION
  PUBLIC
  INHERITING FROM cx_static_check
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES if_t100_dyn_msg .
    INTERFACES if_t100_message .

    DATA guid TYPE guid_16.
    DATA text TYPE string.
    DATA broker_name TYPE zamq_broker_name.

    CONSTANTS:
      BEGIN OF daemon_not_found,
        msgid TYPE symsgid VALUE 'ZAMQ_DAEMON',
        msgno TYPE symsgno VALUE '001',
        attr1 TYPE scx_attrname VALUE 'GUID',
        attr2 TYPE scx_attrname VALUE 'attr2',
        attr3 TYPE scx_attrname VALUE 'attr3',
        attr4 TYPE scx_attrname VALUE 'attr4',
      END OF daemon_not_found,
      BEGIN OF message_not_found,
        msgid TYPE symsgid VALUE 'ZAMQ_DAEMON',
        msgno TYPE symsgno VALUE '002',
        attr1 TYPE scx_attrname VALUE 'GUID',
        attr2 TYPE scx_attrname VALUE 'attr2',
        attr3 TYPE scx_attrname VALUE 'attr3',
        attr4 TYPE scx_attrname VALUE 'attr4',
      END OF message_not_found,
      BEGIN OF message_error,
        msgid TYPE symsgid VALUE 'ZAMQ_DAEMON',
        msgno TYPE symsgno VALUE '003',
        attr1 TYPE scx_attrname VALUE 'TEXT',
        attr2 TYPE scx_attrname VALUE 'attr2',
        attr3 TYPE scx_attrname VALUE 'attr3',
        attr4 TYPE scx_attrname VALUE 'attr4',
      END OF message_error,
      BEGIN OF broker_exists,
        msgid TYPE symsgid VALUE 'ZAMQ_DAEMON',
        msgno TYPE symsgno VALUE '004',
        attr1 TYPE scx_attrname VALUE 'BROKER_NAME',
        attr2 TYPE scx_attrname VALUE 'attr2',
        attr3 TYPE scx_attrname VALUE 'attr3',
        attr4 TYPE scx_attrname VALUE 'attr4',
      END OF broker_exists.

    METHODS constructor
      IMPORTING
        textid      LIKE if_t100_message=>t100key OPTIONAL
        previous    LIKE previous OPTIONAL
        guid        TYPE guid_16 OPTIONAL
        text        TYPE string OPTIONAL
        broker_name TYPE zamq_broker_name OPTIONAL.

ENDCLASS.



CLASS zcx_amq_daemon IMPLEMENTATION.


  METHOD constructor ##ADT_SUPPRESS_GENERATION.
    CALL METHOD super->constructor
      EXPORTING
        previous = previous.
    CLEAR me->textid.
    IF textid IS INITIAL.
      if_t100_message~t100key = if_t100_message=>default_textid.
    ELSE.
      if_t100_message~t100key = textid.
    ENDIF.

    me->guid = guid.
    me->text = text.
    me->broker_name = broker_name.

  ENDMETHOD.

ENDCLASS.
