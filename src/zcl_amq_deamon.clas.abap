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
      IMPORTING i_message TYPE zif_mqtt_packet=>ty_message.

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
      IMPORTING
        i_message       TYPE zif_mqtt_packet=>ty_message
      RETURNING
        VALUE(r_result) TYPE guid_16.

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

    DATA(message_guid) = save_message( i_message ).

    DATA jobcount TYPE btcjobcnt .

    CALL FUNCTION 'JOB_OPEN'
      EXPORTING
*       delanfrep        = space
*       jobgroup         = space
        jobname          = 'ABAPMQ'
*       sdlstrtdt        = NO_DATE
*       sdlstrttm        = NO_TIME
*       jobclass         =
*       check_jobclass   =
      IMPORTING
        jobcount         = jobcount
*       info             =
*      CHANGING
*       ret              =
      EXCEPTIONS
        cant_create_job  = 1
        invalid_job_data = 2
        jobname_missing  = 3
        OTHERS           = 4.

    CHECK sy-subrc = 0.

    SUBMIT zamq_handle_message
      WITH p_mguid = message_guid
      WITH p_dguid = deamon-guid
      USER 'DEVELOPER'
      VIA JOB 'ABAPMQ' NUMBER jobcount
      AND RETURN.

    CALL FUNCTION 'JOB_CLOSE'
      EXPORTING
*       at_opmode            = space
*       at_opmode_periodic   = space
*       calendar_id          = space
*       event_id             = space
*       event_param          = space
*       event_periodic       = space
        jobcount             = jobcount
        jobname              = 'ABAPMQ'
*       laststrtdt           = NO_DATE
*       laststrttm           = NO_TIME
*       prddays              = 0
*       prdhours             = 0
*       prdmins              = 0
*       prdmonths            = 0
*       prdweeks             = 0
*       predjob_checkstat    = space
*       pred_jobcount        = space
*       pred_jobname         = space
*       sdlstrtdt            = sy-datum
*       sdlstrttm            = sy-uzeit
*       startdate_restriction       = BTC_PROCESS_ALWAYS
        strtimmed            = abap_true
*       targetsystem         = space
*       start_on_workday_not_before = SY-DATUM
*       start_on_workday_nr  = 0
*       workday_count_direction     = 0
*       recipient_obj        =
*       targetserver         = space
*       dont_release         = space
*       targetgroup          = space
        direct_start         = abap_true
*       inherit_recipient    =
*       inherit_target       =
*       register_child       = abap_false
*       time_zone            =
*       email_notification   =
*      IMPORTING
*       job_was_released     = job_was_released
*      CHANGING
*       ret                  =
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
      RETURN.
    ENDIF.


  ENDMETHOD.


  METHOD save_message.

    CALL FUNCTION 'GUID_CREATE'
      IMPORTING
        ev_guid_16 = r_result.

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
