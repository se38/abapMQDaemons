*&---------------------------------------------------------------------*
*& Report zamq_send_message_rest
*&---------------------------------------------------------------------*
*& Demo send message via REST interface
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
REPORT zamq_send_message_rest.

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.
  PROTECTED SECTION.
  PRIVATE SECTION.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    cl_http_client=>create_by_url(
      EXPORTING
*        url                    = 'http://192.168.38.xxx:8161/api/message/testTopic.abap?type=topic'
        url                    = 'http://192.168.38.xxx:8161/api/message/testQueue.abap?type=queue'
      IMPORTING
        client                 = DATA(client)
      EXCEPTIONS
        argument_not_found     = 1
        plugin_not_active      = 2
        internal_error         = 3
        pse_not_found          = 4
        pse_not_distrib        = 5
        pse_errors             = 6
        OTHERS                 = 7
    ).
    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    client->authenticate(
      EXPORTING
        username             = 'xxx'
        password             = 'xxx'
    ).

    client->request->set_method( if_http_request=>co_request_method_post ).
    client->request->set_form_field( name = 'body' value = 'yet another test' ).

    client->send(
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3
        http_invalid_timeout       = 4
        OTHERS                     = 5
    ).
    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    client->receive(
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3
        OTHERS                     = 4 ).

    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    " Catch data
    DATA(stream) = client->response->get_cdata( ).

    client->close(
      EXCEPTIONS
        http_invalid_state = 1
        OTHERS             = 2
    ).
    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    cl_demo_output=>display( stream ).

  ENDMETHOD.

ENDCLASS.
