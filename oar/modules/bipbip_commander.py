#!/usr/bin/env python
# coding: utf-8
"""Process that launches and manages bipbip processes
   OAREXEC_REGEXP
   OARRUNJOB_REGEXP
   LEONEXTERMINATE_REGEXP
"""
from oar.lib import (config, get_logger)

import zmq
from  multiprocessing import Process


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'SERVER_HOSTNAME': 'localhost',
    'SERVER_PORT': '6666',
    'DETACH_JOB_FROM_SERVER': '0',
    'LOG_FILE': '/var/log/oar.log',
    'BIPBIP_COMMANDER_SERVER': 'localhost',
    'BIPBIP_COMMANDER_PORT': '6667'
}


def bipbip_executor():
    context = zmq.Context()
    commander_notification = context.socket(zmq.PUSH)


def leon_executor():
    pass

    
def bipbip_commander:
    # Initialize a zeromq context
    context = zmq.Context()
    appendice = context.socket(zmq.PUSH)
    notification = context.socket(zmq.PULL)
    


    
    while (not stop) and

if __name__ == "__main__":
    bipbip_commander()


config.setdefault_config(DEFAULT_CONFIG)

    pipe(pipe_bipbip_read,pipe_bipbip_write);
    autoflush pipe_bipbip_write 1;
    autoflush pipe_bipbip_read 1;

    my $bipbip_launcher_pid=0;
    $bipbip_launcher_pid=fork();
    if ($bipbip_launcher_pid==0){
        #CHILD
        oar_debug("[Almighty][bipbip_launcher] Start bipbip handler process\n");
        close(pipe_bipbip_write);
        $SIG{USR1} = 'IGNORE';
        $SIG{INT}  = 'IGNORE';
        $SIG{TERM} = 'IGNORE';
        $0="Almighty: bipbip";
        # Pipe to handle children ending
        pipe(pipe_bipbip_children_read,pipe_bipbip_children_write);
        autoflush pipe_bipbip_children_write 1;
        autoflush pipe_bipbip_children_read 1;
        # Handle finished bipbip processes
        sub bipbip_child_signal_handler {
            $SIG{CHLD} = \&bipbip_child_signal_handler;
            my $wait_pid_ret ;
            while (($wait_pid_ret = waitpid(-1,WNOHANG)) > 0){
                my $exit_value = $? >> 8;
                print(pipe_bipbip_children_write "$wait_pid_ret $exit_value\n");
            }
        }
        $SIG{CHLD} = \&bipbip_child_signal_handler;

        my $rin_pipe = '';
        vec($rin_pipe,fileno(pipe_bipbip_read),1) = 1;
        my $rin_sig = '';
        vec($rin_sig,fileno(pipe_bipbip_children_read),1) = 1;
        my $rin = $rin_pipe | $rin_sig;
        my $rin_tmp;
        my $stop = 0;
        my %bipbip_children = (); my %bipbip_current_processing_jobs = ();
        my @bipbip_processes_to_run = ();
        while ($stop == 0){ 
            select($rin_tmp = $rin, undef, undef, undef);
            my $current_time = time();
            if (vec($rin_tmp, fileno(pipe_bipbip_children_read), 1)){
                my ($res_read,$line_read) = OAR::Tools::read_socket_line(\*pipe_bipbip_children_read,1);
                if ($line_read =~ m/(\d+) (\d+)/m){
                    my $process_duration = $current_time -  $bipbip_current_processing_jobs{$bipbip_children{$1}}->[1];
                    oar_debug("[Almighty][bipbip_launcher] Process $1 for the job $bipbip_children{$1} ends with exit_code=$2, duration=${process_duration}s\n");
                    delete($bipbip_current_processing_jobs{$bipbip_children{$1}});
                    delete($bipbip_children{$1});
                }else{
                    oar_warn("[Almighty][bipbip_launcher] Read a malformed string in pipe_bipbip_children_read: $line_read\n");
                }
            }elsif (vec($rin_tmp, fileno(pipe_bipbip_read), 1)){
                my ($res_read,$line_read) = OAR::Tools::read_socket_line(\*pipe_bipbip_read,1);
                if (($res_read == 1) and ($line_read eq "")){
                    $stop = 1;
                    oar_warn("[Almighty][bipbip_launcher] Father pipe closed so we stop the process\n");
                }elsif (($line_read =~ m/$OAREXEC_REGEXP/m) or
                        ($line_read =~ m/$OARRUNJOB_REGEXP/m) or
                        ($line_read =~ m/$LEONEXTERMINATE_REGEXP/m)){
                    if (!grep(/^$line_read$/,@bipbip_processes_to_run)){
                        oar_debug("[Almighty][bipbip_launcher] Read on pipe: $line_read\n");
                        push(@bipbip_processes_to_run, $line_read);
                    }
                }else{
                    oar_warn("[Almighty][bipbip_launcher] Read a bad string: $line_read\n");
                }
            }
            my @bipbip_processes_to_requeue = ();
            while(($stop == 0) and ($#bipbip_processes_to_run >= 0) and (keys(%bipbip_children) < $Max_bipbip_processes)){
                my $str = shift(@bipbip_processes_to_run);
                my $cmd_to_run;
                my $bipbip_job_id = 0;
                if ($str =~ m/$OAREXEC_REGEXP/m){
                    $cmd_to_run = "$bipbip_command $1 $2 $3 $4";
                    $bipbip_job_id = $1;
                }elsif ($str =~ m/$OARRUNJOB_REGEXP/m){
                    $cmd_to_run = "$bipbip_command $1";
                    $bipbip_job_id = $1;
                }elsif ($str =~ m/$LEONEXTERMINATE_REGEXP/m){
                    $cmd_to_run = "$leon_command $1";
                    $bipbip_job_id = $1;
                }
                if ($bipbip_job_id > 0){
                    if (defined($bipbip_current_processing_jobs{$bipbip_job_id})){
                        if (!grep(/^$str$/,@bipbip_processes_to_run)){
                            oar_debug("[Almighty][bipbip_launcher] A process is already running for the job $bipbip_job_id. We requeue: $str\n");
                            push(@bipbip_processes_to_requeue, $str);
                        }
                    }else{
                        my $pid=0;
                        $pid=fork;
                        if (!defined($pid)){
                            oar_error("[Almighty][bipbip_launcher] Fork failed, I kill myself\n");
                            exit(2);
                        }
                        if($pid==0){
                            #CHILD
                            $SIG{USR1} = 'IGNORE';
                            $SIG{INT}  = 'IGNORE';
                            $SIG{TERM} = 'IGNORE';
                            $SIG{CHLD} = 'DEFAULT';
                            open (STDIN, "</dev/null");
                            open (STDOUT, ">> $Log_file");
                            open (STDERR, ">&STDOUT");
                            exec("$cmd_to_run");
                            oar_error("[Almighty][bipbip_launcher] failed exec: $cmd_to_run\n");
                            exit(1);
                        }
                        $bipbip_current_processing_jobs{$bipbip_job_id} = [$pid, $current_time];
                        $bipbip_children{$pid} = $bipbip_job_id;
                        oar_debug("[Almighty][bipbip_launcher] Run process: $cmd_to_run\n");
                    }
                }else{
                    oar_warn("[Almighty][bipbip_launcher] Bad string read in the bipbip queue: $str\n");
                }
            }
            push(@bipbip_processes_to_run, @bipbip_processes_to_requeue);
            oar_debug("[Almighty][bipbip_launcher] Nb running bipbip: ".keys(%bipbip_children)."/$Max_bipbip_processes; Waiting processes(".($#bipbip_processes_to_run + 1)."): @bipbip_processes_to_run\n");
            # Check if some bipbip processes are blocked; this must never happen
            if ($Detach_oarexec != 0){
                foreach my $b (keys(%bipbip_current_processing_jobs)){
                    my $process_duration = $current_time -  $bipbip_current_processing_jobs{$b}->[1];
                    oar_debug("[Almighty][bipbip_launcher] Check bipbip process duration: job=$b, pid=$bipbip_current_processing_jobs{$b}->[0], time=$bipbip_current_processing_jobs{$b}->[1], current_time=$current_time, duration=${process_duration}s\n");
                    if ($bipbip_current_processing_jobs{$b}->[1] < ($current_time - $Max_bipbip_process_duration)){
                        oar_warn("[Almighty][bipbip_launcher] Max duration for the bipbip process $bipbip_current_processing_jobs{$b}->[0] reached (${Max_bipbip_process_duration}s); job $b\n");
                        kill(9, $bipbip_current_processing_jobs{$b}->[0]);
                    }
                }
            }
        }
        oar_warn("[Almighty][bipbip_launcher] End of process\n");
        exit(1);
    }

    close(pipe_bipbip_read);
    while (1){
        my $answer = qget_appendice();
        oar_debug("[Almighty] Appendice has read on the socket : $answer\n");
        if (($answer =~ m/$OAREXEC_REGEXP/m) or
            ($answer =~ m/$OARRUNJOB_REGEXP/m) or
            ($answer =~ m/$LEONEXTERMINATE_REGEXP/m)){
            if (! print pipe_bipbip_write "$answer\n"){
                oar_error("[Almighty] Appendice cannot communicate with bipbip_launcher process, I kill myself\n");
                exit(2);
            }
            flush pipe_bipbip_write;
        }elsif ($answer ne ""){
            print WRITE "$answer\n";
            flush WRITE;
        }else{
            oar_debug("[Almighty] A connection was opened but nothing was written in the socket\n");
            #sleep(1);
        }
    }
}
