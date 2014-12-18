from oar import config, logging
from platform import Platform

#
# Karma and Fairsharing stuff 
#                             

fairsharing_nb_job_limit = config["SCHEDULER_FAIRSHARING_MAX_JOB_PER_USER"]
karma_window_size =  3600 * 30 * 24 # TODO in conf ???

# defaults values for fairsharing 
k_proj_targets = "{default => 21.0}"
k_user_targets = "{default => 22.0}"
k_coeff_proj_consumption = "0"
k_coeff_user_consumption = "1"
k_karma_coeff_user_asked_consumption = "1"
#(* get fairsharing config if any *)
#let karma_proj_targets = Conf.str_perl_hash_to_pairs_w_convert (Conf.get_default_value "SCHEDULER_FAIRSHARING_PROJECT_TARGETS" k_proj_targets) float_of_string_e
#let karma_user_targets = Conf.str_perl_hash_to_pairs_w_convert (Conf.get_default_value "SCHEDULER_FAIRSHARING_USER_TARGETS" k_user_targets) float_of_string_e
#let karma_coeff_proj_consumption = float_of_string_e (Conf.get_default_value "SCHEDULER_FAIRSHARING_COEF_PROJECT" k_coeff_proj_consumption)
#let karma_coeff_user_consumption = float_of_string_e (Conf.get_default_value "SCHEDULER_FAIRSHARING_COEF_USER" k_coeff_user_consumption)
#let karma_coeff_user_asked_consumption = float_of_string_e (Conf.get_default_value "SCHEDULER_FAIRSHARING_COEF_USER_ASK" k_karma_coeff_user_asked_consumption)

#(*                                                     *)
#(* Sort jobs accordingly to karma value (fairsharing)  *)
#(*                                                     *)
#let jobs_karma_sorting dbh queue now karma_window_size jobs_ids h_jobs =
#  let start_window = Int64.sub now karma_window_size and stop_window = now in
#    let karma_sum_time_asked, karma_sum_time_used = Iolib.get_sum_accounting_window dbh queue start_window stop_window
#    and karma_projects_asked, karma_projects_used = Iolib.get_sum_accounting_for_param dbh queue "accounting_project" start_window stop_window
#    and karma_users_asked, karma_users_used       = Iolib.get_sum_accounting_for_param dbh queue "accounting_user" start_window stop_window
#    in
#      let karma j = let job = try Hashtbl.find h_jobs j  with Not_found -> failwith "Karma: not found job" in
#        let user = job.user and proj = job.project in
#        let karma_proj_used_j  = try Hashtbl.find karma_projects_used proj  with Not_found -> 0.0
#        and karma_user_used_j  = try Hashtbl.find karma_users_used user  with Not_found -> 0.0
#        and karma_user_asked_j = try Hashtbl.find karma_users_asked user  with Not_found -> 0.0
#        (* TODO test *)
#        and karma_proj_target =  try List.assoc proj karma_proj_targets with Not_found -> 0.0 (* TODO  verify in perl 0 also ? *)
#        and karma_user_target = (try List.assoc user karma_user_targets with Not_found -> 0.0  ) /. 100.0 (* TODO   verify in perl 0 also ? *)
#        in
#          karma_coeff_proj_consumption *. ((karma_proj_used_j /. karma_sum_time_used) -. (karma_proj_target /. 100.0)) +.
#          karma_coeff_user_consumption *. ((karma_user_used_j /. karma_sum_time_used) -. (karma_user_target /. 100.0)) +.
#          karma_coeff_user_asked_consumption *. ((karma_user_asked_j /. karma_sum_time_asked) -. (karma_user_target /. 100.0))
#      in
#      let kompare x y = let kdiff = (karma x) -. (karma y) in if kdiff = 0.0 then 0 else if kdiff > 0.0 then 1 else -1 in
#        List.sort kompare jobs_ids;;

# Sort jobs accordingly to karma value (fairsharing)  *)
