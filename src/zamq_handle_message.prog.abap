*&---------------------------------------------------------------------*
*& Report zamq_handle_message
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
********************************************************************************
* The MIT License (MIT)
*
* Copyright (c) 2021 Uwe Fetzer and Contributors
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
REPORT zamq_handle_message.

PARAMETERS: p_mguid TYPE guid_16.     "Message GUID
PARAMETERS: p_dguid TYPE guid_16.     "Deamon GUID

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.

  PROTECTED SECTION.
  PRIVATE SECTION.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    TRY.
        DATA(deamon) = zcl_amq_deamon=>get_deamon( p_dguid ).

        DATA(handler_class_name) = deamon->get_handler_class( ).
        DATA handler TYPE REF TO zif_amq_deamon.
        CREATE OBJECT handler TYPE (handler_class_name).

*        handler->on_receive(
*          EXPORTING
*            i_message     =
**            i_deamon_guid =
*        ).
      CATCH zcx_amq_deamon
            cx_sy_create_object_error INTO DATA(lcx).
        MESSAGE lcx TYPE 'E'.
    ENDTRY.

  ENDMETHOD.

ENDCLASS.
