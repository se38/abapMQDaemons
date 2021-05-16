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
PARAMETERS: p_dguid TYPE guid_16.     "Daemon GUID

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.

  PRIVATE SECTION.
    METHODS get_message
      IMPORTING i_message_guid  TYPE guid_16
      RETURNING VALUE(r_result) TYPE zif_mqtt_packet=>ty_message
      RAISING   zcx_amq_daemon.
    METHODS delete_message
      IMPORTING i_message_guid TYPE guid_16.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    TRY.
        DATA(daemon) = zcl_amq_daemon=>get_daemon( p_dguid ).

        DATA(handler_class_name) = daemon->get_handler_class( ).
        DATA handler TYPE REF TO zif_amq_daemon.
        CREATE OBJECT handler TYPE (handler_class_name).

        handler->on_receive(
          EXPORTING
            i_message     = get_message( p_mguid )
            i_daemon_guid = p_dguid
        ).

        delete_message( p_mguid ).

      CATCH zcx_amq_daemon
            cx_sy_create_object_error INTO DATA(lcx).
        MESSAGE lcx TYPE 'E'.
    ENDTRY.

  ENDMETHOD.

  METHOD get_message.

    SELECT SINGLE topic, message
      INTO @r_result
      FROM zamq_messages
      WHERE guid = @i_message_guid.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_amq_daemon
        EXPORTING
          textid = zcx_amq_daemon=>message_not_found
          guid   = i_message_guid.
    ENDIF.

  ENDMETHOD.

  METHOD delete_message.

    DELETE FROM zamq_messages
      WHERE guid = @i_message_guid.

  ENDMETHOD.

ENDCLASS.
