CLASS zcl_amq_deamon DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE .

  PUBLIC SECTION.
    "! <p class="shorttext synchronized" lang="en">Reads definition from DB and returns des Deamon as object</p>
    "! @parameter i_dguid | <p class="shorttext synchronized" lang="en">Deamon GUID</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Deamon</p>
    "! @raising zcx_amq_deamon | <p class="shorttext synchronized" lang="en"></p>
    CLASS-METHODS get_deamon
      IMPORTING i_dguid         TYPE guid_16
      RETURNING VALUE(r_result) TYPE REF TO zcl_amq_deamon
      RAISING   zcx_amq_deamon.

    "! <p class="shorttext synchronized" lang="en">Returns the Brokername of the Deamon</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Brokername</p>
    METHODS get_broker_name
      RETURNING VALUE(r_result) TYPE zamq_broker_name.

    "! <p class="shorttext synchronized" lang="en">Returns the topics of the Deamon as string table</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Topics</p>
    METHODS get_topics
      RETURNING VALUE(r_result) TYPE string_table.

    "! <p class="shorttext synchronized" lang="en">Returns the Deamonname</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Deamonname</p>
    METHODS get_deamon_name
      RETURNING VALUE(r_result) TYPE zamq_deamon_name.

    "! <p class="shorttext synchronized" lang="en">Handle incoming messages</p>
    "! @parameter i_message | <p class="shorttext synchronized" lang="en">Message</p>
    METHODS handle_message
      IMPORTING i_message TYPE zif_mqtt_packet=>ty_message
      RAISING   zcx_amq_deamon.

    "! <p class="shorttext synchronized" lang="en">Returns the handler classname</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Handler classname</p>
    METHODS get_handler_class
      RETURNING VALUE(r_result) TYPE seoclsname.

  PRIVATE SECTION.
    DATA deamon TYPE zamq_deamons.

    METHODS get_deamon_db
      IMPORTING i_dguid         TYPE guid_16
      RETURNING VALUE(r_result) TYPE zamq_deamons
      RAISING   zcx_amq_deamon.
    METHODS save_message
      IMPORTING i_message       TYPE zif_mqtt_packet=>ty_message
      RETURNING VALUE(r_result) TYPE sysuuid_x16
      RAISING   cx_uuid_error.

ENDCLASS.

CLASS zcl_amq_deamon IMPLEMENTATION.

  METHOD get_deamon.

    r_result = NEW #( ).
    r_result->deamon = r_result->get_deamon_db( i_dguid ).

  ENDMETHOD.

  METHOD get_deamon_db.

    SELECT SINGLE * FROM zamq_deamons
      INTO @r_result
      WHERE guid = @i_dguid.

    IF sy-subrc <> 0.
      RAISE EXCEPTION TYPE zcx_amq_deamon
        EXPORTING
          textid = zcx_amq_deamon=>deamon_not_found
          guid   = i_dguid.
    ENDIF.

  ENDMETHOD.

  METHOD get_broker_name.
    r_result = deamon-broker_name.
  ENDMETHOD.

  METHOD get_topics.
    SPLIT deamon-topics AT ',' INTO TABLE r_result.
  ENDMETHOD.

  METHOD get_deamon_name.
    r_result = deamon-deamon_name.
  ENDMETHOD.

  METHOD handle_message.

    TRY.
        DATA(message_guid) = save_message( i_message ).
      CATCH cx_uuid_error INTO DATA(lcx).
        RAISE EXCEPTION TYPE zcx_amq_deamon
          EXPORTING
            textid = zcx_amq_deamon=>message_error
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
      RAISE EXCEPTION TYPE zcx_amq_deamon
        EXPORTING
          textid = zcx_amq_deamon=>message_error
          text   = |JOB_OPEN RC = { sy-subrc }|.
    ENDIF.

    SUBMIT zamq_handle_message
      WITH p_mguid = message_guid
      WITH p_dguid = deamon-guid
      USER 'DEVELOPER'
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
      RAISE EXCEPTION TYPE zcx_amq_deamon
        EXPORTING
          textid = zcx_amq_deamon=>message_error
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

    INSERT zamq_messages FROM message.

  ENDMETHOD.

  METHOD get_handler_class.
    r_result = deamon-handler_class.
  ENDMETHOD.

ENDCLASS.
