delayed_resque
==============

Provides delayed_job syntax to resque.

Call `.delay.method(params)` on any object and it will be processed in the background.

    # with delayed_job
    @user.delay.activate!(@device)

Parameters can be scalar values, active record instances, or classes (but not instances of non-AR objects).

The queue to use for the method can be specified on the delay method:

    @user.delay(:queue => :device_activation).activate!(@device)

Credits
-------

Based on the work of https://github.com/defunkt/resque and 
https://github.com/collectiveidea/delayed_job.