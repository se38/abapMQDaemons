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
"! <p class="shorttext synchronized" lang="en">abapMQ Daemon</p>
CLASS zcl_amq_daemon DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE .

  PUBLIC SECTION.
    "! <p class="shorttext synchronized" lang="en">Reads definition from DB and returns des daemon as object</p>
    "! @parameter i_dguid | <p class="shorttext synchronized" lang="en">daemon GUID</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">daemon</p>
    "! @raising zcx_amq_daemon | <p class="shorttext synchronized" lang="en"></p>
    CLASS-METHODS get_daemon
      IMPORTING i_dguid         TYPE guid_16
      RETURNING VALUE(r_result) TYPE REF TO zcl_amq_daemon
      RAISING   zcx_amq_daemon.

    "! <p class="shorttext synchronized" lang="en">Returns the Brokername of the daemon</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Brokername</p>
    METHODS get_broker_name
      RETURNING VALUE(r_result) TYPE zamq_broker_name.

    "! <p class="shorttext synchronized" lang="en">Returns the topics of the daemon as string table</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Topics</p>
    METHODS get_topics
      RETURNING VALUE(r_result) TYPE string_table.

    "! <p class="shorttext synchronized" lang="en">Returns the daemonname</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">daemonname</p>
    METHODS get_daemon_name
      RETURNING VALUE(r_result) TYPE zamq_daemon_name.

    "! <p class="shorttext synchronized" lang="en">Handle incoming messages</p>
    "! @parameter i_message | <p class="shorttext synchronized" lang="en">Message</p>
    METHODS handle_message
      IMPORTING i_message TYPE zif_mqtt_packet=>ty_message
      RAISING   zcx_amq_daemon.

    "! <p class="shorttext synchronized" lang="en">Returns the handler classname</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Handler classname</p>
    METHODS get_handler_class
      RETURNING VALUE(r_result) TYPE seoclsname.

    "! <p class="shorttext synchronized" lang="en">Start daemon</p>
    "!
    "! @parameter i_stop | <p class="shorttext synchronized" lang="en">Stop message</p>
    "! @parameter i_user | <p class="shorttext synchronized" lang="en">Broker user</p>
    "! @parameter i_pass | <p class="shorttext synchronized" lang="en">Broker password</p>
    METHODS start
      IMPORTING i_stop TYPE zamq_stop_message DEFAULT 'STOP'
                i_user TYPE zamq_user
                i_pass TYPE zamq_password.

  PRIVATE SECTION.
    DATA daemon TYPE zamq_daemons.
    DATA appl_log TYPE REF TO zcl_amq_appl_log.

    METHODS get_daemon_db
      IMPORTING i_dguid         TYPE guid_16
      RETURNING VALUE(r_result) TYPE zamq_daemons
      RAISING   zcx_amq_daemon.
    METHODS save_message
      IMPORTING i_message       TYPE zif_mqtt_packet=>ty_message
      RETURNING VALUE(r_result) TYPE sysuuid_x16
      RAISING   cx_uuid_error.

    METHODS get_broker
      IMPORTING i_brokername    TYPE zamq_broker_name
      RETURNING VALUE(r_result) TYPE zamq_broker.
    METHODS create_tcp_transport
      IMPORTING i_broker        TYPE zamq_broker
      RETURNING VALUE(r_result) TYPE REF TO zif_mqtt_transport
      RAISING   cx_apc_error.
    METHODS wait_and_process
      IMPORTING i_tcp           TYPE REF TO zif_mqtt_transport
                i_timeout       TYPE i
                i_stop          TYPE zamq_stop_message
      RETURNING VALUE(r_result) TYPE string
      RAISING   zcx_mqtt
                zcx_amq_daemon.

ENDCLASS.

CLASS zcl_amq_daemon IMPLEMENTATION.

  METHOD get_daemon.

    r_result = NEW #( ).
    r_result->daemon = r_result->get_daemon_db( i_dguid ).

  ENDMETHOD.

  METHOD get_daemon_db.

    SELECT SINGLE * FROM zamq_daemons
      INTO @r_result
      WHERE guid = @i_dguid.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_amq_daemon
        EXPORTING
          textid = zcx_amq_daemon=>daemon_not_found
          guid   = i_dguid.
    ENDIF.

  ENDMETHOD.

  METHOD get_broker_name.
    r_result = daemon-broker_name.
  ENDMETHOD.

  METHOD get_topics.
    SPLIT daemon-topics AT ',' INTO TABLE r_result.

    "swap slash and dot
    LOOP AT r_result REFERENCE INTO DATA(topic).
      TRANSLATE topic->* USING '/../+**+'.
    ENDLOOP.

  ENDMETHOD.

  METHOD get_daemon_name.
    r_result = daemon-daemon_name.
  ENDMETHOD.

  METHOD handle_message.

    TRY.
        DATA(message_guid) = save_message( i_message ).
      CATCH cx_uuid_error INTO DATA(lcx).
        RAISE EXCEPTION TYPE zcx_amq_daemon
          EXPORTING
            textid = zcx_amq_daemon=>message_error
            text   = lcx->get_text( ).
    ENDTRY.

    DATA jobcount TYPE btcjobcnt .

    CALL FUNCTION 'JOB_OPEN'
      EXPORTING
        jobname          = 'ABAPMQ'
      IMPORTING
        jobcount         = jobcount
      EXCEPTIONS
        cant_create_job  = 1
        invalid_job_data = 2
        jobname_missing  = 3
        OTHERS           = 4.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_amq_daemon
        EXPORTING
          textid = zcx_amq_daemon=>message_error
          text   = |JOB_OPEN RC = { sy-subrc }|.
    ENDIF.

    SUBMIT zamq_handle_message
      WITH p_mguid = message_guid
      WITH p_dguid = daemon-guid
      USER daemon-bgr_user
      VIA JOB 'ABAPMQ' NUMBER jobcount
      AND RETURN.

    CALL FUNCTION 'JOB_CLOSE'
      EXPORTING
        jobcount             = jobcount
        jobname              = 'ABAPMQ'
        strtimmed            = abap_true
        direct_start         = abap_true
      EXCEPTIONS
        cant_start_immediate = 1
        invalid_startdate    = 2
        jobname_missing      = 3
        job_close_failed     = 4
        job_nosteps          = 5
        job_notex            = 6
        lock_failed          = 7
        invalid_target       = 8
        invalid_time_zone    = 9
        OTHERS               = 10.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_amq_daemon
        EXPORTING
          textid = zcx_amq_daemon=>message_error
          text   = |JOB_CLOSE RC = { sy-subrc }|.
    ENDIF.

  ENDMETHOD.

  METHOD save_message.

    r_result = cl_system_uuid=>create_uuid_x16_static( ).

    DATA(message) = VALUE zamq_messages(
      guid = r_result
      topic = i_message-topic
      message = i_message-message
    ).

    "swap dot and slash, plus and star
    TRANSLATE message-topic USING '/../+**+'.
    INSERT zamq_messages FROM @message.

  ENDMETHOD.

  METHOD get_handler_class.
    r_result = daemon-handler_class.
  ENDMETHOD.

  METHOD get_broker.

    SELECT SINGLE * FROM zamq_broker
      INTO @r_result
      WHERE broker_name = @i_brokername.

  ENDMETHOD.

  METHOD create_tcp_transport.

    r_result = zcl_mqtt_transport_tcp=>create(
      iv_host = i_broker-broker_host
      iv_port = CONV #( i_broker-broker_port )
      iv_protocol = SWITCH #( i_broker-use_ssl
                                WHEN abap_true
                                THEN cl_apc_tcp_client_manager=>co_protocol_type_tcps
                                ELSE cl_apc_tcp_client_manager=>co_protocol_type_tcp
                            )
    ).

  ENDMETHOD.

  METHOD wait_and_process.

    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    " wait an hour for new messages
    " new message -> STOP message? -> disconnect and exit
    "             -> else process message in handler class and wait again
    " after one hour or timeout by broker -> reconnect
    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    DO.
      DATA(message) = CAST zcl_mqtt_packet_publish( i_tcp->listen( i_timeout ) )->get_message( ).
      r_result = cl_binary_convert=>xstring_utf8_to_string( message-message ).

      IF r_result = i_stop.
        EXIT.
      ENDIF.

      handle_message( message ).
    ENDDO.

  ENDMETHOD.

  METHOD start.

    appl_log = NEW #( ).

    DATA(broker) = get_broker( get_broker_name(  ) ).
    DATA(topic_strings) = get_topics( ).
    DATA(topics) = VALUE zcl_mqtt_packet_subscribe=>ty_topics( FOR topic IN topic_strings ( topic = topic ) ).

    DATA(log_handle) = appl_log->add_message( |daemon { get_daemon_name( ) } started at { sy-datlo DATE = USER } { sy-timlo TIME = USER }| ).

    TRY.
        DO.
          DATA(tcp) = create_tcp_transport( broker ).

          tcp->connect( ).

          TRY.
              tcp->send( NEW zcl_mqtt_packet_connect( iv_username = i_user iv_password = i_pass ) ).

              DATA(connack) = CAST zcl_mqtt_packet_connack( tcp->listen( 10 ) ).

              tcp->send( NEW zcl_mqtt_packet_subscribe(
                it_topics            = topics
                iv_packet_identifier = '0001' ) ).

              DATA(return_codes) = CAST zcl_mqtt_packet_suback( tcp->listen( 30 ) )->get_return_codes( ).

            CATCH zcx_mqtt_timeout.
              EXIT.
          ENDTRY.

          TRY.
              IF return_codes[ 1 ] = '00'.

                """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                " wait an hour for new messages
                " new message -> STOP message? -> disconnect and exit
                "             -> else process message in handler class and wait again
                " after one hour or timeout by broker -> reconnect
                """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                DATA(message_string) = wait_and_process(
                  i_tcp = tcp
                  i_timeout = 3600
                  i_stop = i_stop
                ).

              ELSE.
                appl_log->add_message(
                  i_text = |Connection Error, Returncode' { return_codes[ 1 ] }|
                  i_message_type = 'E'
                ).
              ENDIF.
              """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

              tcp->send( NEW zcl_mqtt_packet_disconnect( ) ).
              tcp->disconnect( ).

              IF message_string = i_stop
              OR return_codes[ 1 ] <> '00'.
                EXIT.
              ENDIF.

            CATCH zcx_mqtt_timeout ##no_handler.
          ENDTRY.
        ENDDO.

      CATCH cx_apc_error
            zcx_amq_daemon
            zcx_mqtt INTO DATA(lcx_apc).
        appl_log->add_message(
          i_message_type = 'E'
          i_text         = lcx_apc->get_text( ) ).
        EXIT.
    ENDTRY.

    appl_log->add_message( |daemon stopped at { sy-datlo DATE = USER } { sy-timlo TIME = USER }| ).

  ENDMETHOD.

ENDCLASS.
