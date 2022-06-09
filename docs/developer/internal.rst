Technical details and diagrams
==============================

One part of the complexity of oar comes from its distributes nature. Sequence diagrams help to understand the temporality of the actions done by oar to perform specific actions.
Note, that sequence diagram are helpful to visualize chain of action in the time, however it shows a specific scenario, and thus is not exhaustive.

Job lifetime
------------

This diagram illustrate the job lifetime in the system. The user is connected with `ssh` on a frontend. The ``Automaton``, ``Scheduler`` and ``Launchers`` are oar modules running on the server. ``Head node`` and ``Nodes`` are the nodes allocated to the job.

1. The job is first submitted by an user using the command ``oarsub``. This command executes locally the checks on the job, and executes the admission rules configured. Once the job has been processed, it is inserted into the database. Additionally, a notification is sent to the ``automaton`` (`oar/modules/almighty.py`).
2. A scheduling phase starts, to process waiting jobs. The jobs are directly fetched in the database.
3. At the end of the scheduling, the scheduler notifies the automaton to start the job that are ready by starting a ``launcher`` (`oar/modules/bipbip.py`).
4. The launcher retrieves information about the job from the database (the allocated node, the user command etc). Before starting the job, the ``launcher`` inits the nodes (using the script `oar/tools/job_resource_manager_cgroups.pl`). Before starting  the job, the ``launcher`` runs a `server prologue` (if configured) on the server.
5. One node is selected as the ``Head node`` which holds the main job's process ``oarexec`` (`oar/tools/oarexec`). ``oarexec`` is responsible for launching the job's prologue (if configured by an administrator) and the user command.
6. In the presented scenario, the user's command finishes. It is detected by ``oarexec`` as the end of the job. ``oarexec`` launches the job's epilogue if configured, and notifies the ``automaton`` for the end of the job.
7. Finally, a ``laucher`` is start to handle the end of the job: it runs a server prologue if necessary, and clean the nodes used by the job.


.. figure:: ./../_static/oar_job_lifetime.png
   :target: ./../_static/oar_job_lifetime.svg
   :alt: Sequence diagram about job execution lifetime

   Sequence diagram of the lifetime of a job. Click on the image to see a bigger version.


.. figure:: ./../_static/oar_execution_chain.png
   :target: ./../_static/oar_execution_chain.svg
   :alt: Sequence diagram about job execution

   Sequence diagram of the mechanisms used by oar to launch a job. Click on the image to see a bigger version.

