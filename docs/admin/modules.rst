.. _modules_reference:

Modules descriptions
====================

OAR can be decomposed into several modules which perform different tasks.

.. _module-almighty-anchor:
.. _almighty_reference:

Almighty
--------

.. automodule:: oar.modules.almighty
  :noindex:


It's behaviour is represented in these schemes, general schema:

  .. image:: ../_static/almighty_automaton_general.png

When the Almighty automaton starts it will first open a socket and creates a
pipe for the process communication with it's forked son. Then, Almighty will
fork itself in a process called "appendice" (:mod:`oar.modules.appendice_proxy`) which role is to listen to incoming
connections on the socket and catch clients messages. These messages will be
thereafter piped to Almighty. Then, the automaton will change it's state
according to what message has been received.

--------------------------------------------------------------------------------

  - Scheduler schema:

  .. image:: ../_static/almighty_automaton_scheduler_part.png

--------------------------------------------------------------------------------

  - Finaud schema:

  .. image:: ../_static/almighty_automaton_finaud_part.png

--------------------------------------------------------------------------------

  - Leon schema:

  .. image:: ../_static/almighty_automaton_leon_part.png

--------------------------------------------------------------------------------

  - Sarko schema:

  .. image:: ../_static/almighty_automaton_villains_part.png

--------------------------------------------------------------------------------

  - ChangeNode schema:

  .. image:: ../_static/almighty_automaton_changenode_part.png

Sarko
-----

.. automodule:: oar.modules.sarko
  :noindex:

Bipbip commander
----------------

.. automodule:: oar.modules.bipbip_commander
  :noindex:

Bipbip
------

.. automodule:: oar.modules.bipbip
  :noindex:


Leon
----

.. automodule:: oar.modules.leon
  :noindex:

Judas
-----

This is the module dedicated to print and log every debugging, warning and
error messages.

The notification functions are the following:

  - send_mail(mail_recipient_address, object, body, job_id) that sends
    emails to the OAR admin

  - notify_user(base, method, host, user, job_id, job_name, tag, comments)
    that parses the notify method. This method can be a user script or a
    mail to send. If the "method" field begins with
    "mail:", notify_user will send an email to the user. If the
    beginning is "exec:", it will execute the script as the "user".

The main logging functions are the following:

  - redirect_everything() this function redirects STDOUT and STDERR into
    the log file

  - oar_debug(message)

  - oar_warn(message)

  - oar_error(message)

The three last functions are used to set the log level of the message.

NodeChangeState
---------------

.. automodule:: oar.modules.node_change_state
  :noindex:

Scheduler
---------

This module checks for each reservation jobs if it is valid and launches them
at the right time.

Scheduler launches all gantt scheduler in the order of the priority specified
in the database and update all visualization tables
(:ref:`database-gantt-jobs-predictions-visu-anchor` and :ref:`database-gantt-jobs-resources-visu-anchor`).

It also trigger if a job has to be launched.

oar_sched_gantt_with_timesharing
________________________________

This is a OAR scheduler. It implements functionalities like
timesharing, moldable jobs, `besteffort jobs`, ...

We have implemented the FIFO with backfilling algorithm. Some parameters
can be changed in the :doc:`configuration file <configuration>` (see
:ref:`SCHEDULER_TIMEOUT <SCHEDULER_TIMEOUT>`,
:ref:`SCHEDULER_JOB_SECURITY_TIME <SCHEDULER_JOB_SECURITY_TIME>`,
:ref:`SCHEDULER_GANTT_HOLE_MINIMUM_TIME <SCHEDULER_GANTT_HOLE_MINIMUM_TIME>`,
:ref:`SCHEDULER_RESOURCE_ORDER <SCHEDULER_RESOURCE_ORDER>`).

oar_sched_gantt_with_timesharing_and_fairsharing
________________________________________________

This scheduler is the same than oar_sched_gantt_with_timesharing_ but it looks
at the consumption past and try to order waiting jobs with fairsharing in mind.

Some parameters can be changed directly in the file

::

    ###############################################################################
    # Fairsharing parameters #
    ##########################
    # Avoid problems if there are too many waiting jobs
    my $Karma_max_number_of_jobs_treated = 1000;
    # number of seconds to consider for the fairsharing
    my $Karma_window_size = 3600 * 30;
    # specify the target percentages for project names (0 if not specified)
    my $Karma_project_targets = {
        first => 75,
        default => 25
    };

    # specify the target percentages for users (0 if not specified)
    my $Karma_user_targets = {
        oar => 100
    };
    # weight given to each criteria
    my $Karma_coeff_project_consumption = 3;
    my $Karma_coeff_user_consumption = 2;
    my $Karma_coeff_user_asked_consumption = 1;
    ###############################################################################


This scheduler takes its historical data in the :ref:`database-accounting-anchor` table. To fill this,
the command :doc:`commands/oaraccounting` has to be run periodically (in a cron job for
example). Otherwise the scheduler cannot be aware of new user consumptions.

oar_sched_gantt_with_timesharing_and_fairsharing_and_quotas
___________________________________________________________

This scheduler is the same than
oar_sched_gantt_with_timesharingand_fairsharing but it implements quotas which
are configured in "/etc/oar/scheduler_quotas.conf".

Greta
-----

.. automodule:: oar.modules.greta
  :noindex:

--------------------------------------------------------------------------------

  - Greta general commands process schema:

  .. image:: ../_static/greta_general_commands_process.png

When Greta is activated, the metascheduler sends, each time it is executed, a
list of nodes that need to be woken-up or may be halted. Greta maintains a
list of commands that have already been sent to the nodes and asks to the
windowforker to actually execute the commands only when it is appropriate.
A special feature is the "keepalive" of nodes depending on some properties:
even if the metascheduler asks to shut-down some nodes, it's up to Greta to
check if the keepalive constraints are still satisfied. If not, Greta refuses
to halt the corresponding nodes.

--------------------------------------------------------------------------------

  - Greta checking process schema:

  .. image:: ../_static/greta_checking_process.png

Greta is called each time the metascheduler is called, to do all the checking
process. This process is also executed when Greta receives normal halt or wake-up
commands from the scheduler. Greta checks if waking-up nodes are actually Alive
or not and suspects the nodes if they haven't woken-up before the timeout.
It also checks keepalive constraints and decides to wake-up nodes if a constraint
is no more satisfied (for example because new jobs are running on nodes that are
now busy, and no more idle).
Greta also checks the results of the commands sent by the windowforker and may
also suspect a node if the command exited with non-zero status.

--------------------------------------------------------------------------------

  - Greta wake-up process schema

  .. image:: ../_static/greta_wakeup_process.png

--------------------------------------------------------------------------------

  - Greta shutdown process schema

  .. image:: ../_static/greta_shutdown_process.png

