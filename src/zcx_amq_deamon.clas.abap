CLASS zcx_amq_deamon DEFINITION
  PUBLIC
  INHERITING FROM cx_static_check
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES if_t100_dyn_msg .
    INTERFACES if_t100_message .

    CONSTANTS:
      BEGIN OF deamon_not_found,
        msgid TYPE symsgid VALUE 'ZCX_AMQ_DEAMON',
        msgno TYPE symsgno VALUE '001',
        attr1 TYPE scx_attrname VALUE 'GUID',
        attr2 TYPE scx_attrname VALUE 'attr2',
        attr3 TYPE scx_attrname VALUE 'attr3',
        attr4 TYPE scx_attrname VALUE 'attr4',
      END OF deamon_not_found,
      BEGIN OF message_not_found,
        msgid TYPE symsgid VALUE 'ZCX_AMQ_DEAMON',
        msgno TYPE symsgno VALUE '002',
        attr1 TYPE scx_attrname VALUE 'GUID',
        attr2 TYPE scx_attrname VALUE 'attr2',
        attr3 TYPE scx_attrname VALUE 'attr3',
        attr4 TYPE scx_attrname VALUE 'attr4',
      END OF message_not_found.

    METHODS constructor
      IMPORTING
        textid   LIKE if_t100_message=>t100key OPTIONAL
        previous LIKE previous OPTIONAL
        guid    TYPE guid_16.

  PRIVATE SECTION.
    DATA guid TYPE guid_16.

ENDCLASS.



CLASS zcx_amq_deamon IMPLEMENTATION.


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

  ENDMETHOD.

ENDCLASS.
