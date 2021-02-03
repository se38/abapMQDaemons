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

    "! <p class="shorttext synchronized" lang="en">Sets the log handle in the Deamon DB</p>
    "! @parameter i_log_handle | <p class="shorttext synchronized" lang="en">Log handle</p>
    METHODS set_log_handle
      IMPORTING i_log_handle TYPE balloghndl.

    "! <p class="shorttext synchronized" lang="en">Returns the current log handle</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Log handle</p>
    METHODS get_log_handle
      RETURNING VALUE(r_result) TYPE balloghndl.

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
          dguid  = i_dguid.
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

  METHOD set_log_handle.
    UPDATE zamq_deamons
      SET log_handle = @i_log_handle
      WHERE guid = @deamon-guid.
  ENDMETHOD.

  METHOD get_log_handle.
    r_result = deamon-log_handle.
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
