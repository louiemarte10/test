<?php
if ($_SERVER['REMOTE_ADDR'] == '192.168.60.229') {
	require("{$_SERVER['DOCUMENT_ROOT']}/config/pipeline-x-v2-2-1.php");
} else {
	require("{$_SERVER['DOCUMENT_ROOT']}/config/pipeline-x.php");
}

//configg::connect(db::)
//require(config::path("tm-targets.php"));
$db = new px_dbase();
config::connect_db($db, "report", "callbox_pipeline2");
config::connect_db(db::$db, "report", "callbox_pipeline2", "reports");
$dot45 = new MySQLi(config::get_server_by_name('main'), "db_pipe", "0ldn3w5", "callbox_pipeline2");
$cdr_ilo = new MySQLi("192.168.50.22", "app_pipe", "a33-pipe", "callpacer");
$cdr_dvo = new MySQLi("192.168.150.22", "app_asterisk", "a33-asterisk", "callpacer");
px_h3_v3::init();
$user_id = px_login::get_info('user_id');
$branch = px_login::get_info('branch');

$logged_user = $user_id;
/* if ($user_id == 1241435) {
	$user_id = 57;
} */

$hids = '';
$dm_r = array();
$dmr_evt_states = array();
$channel_actions = array();
$groups = array();
$node = px_h3_v3::get_user_node($user_id);
$user_node = px_h3_v3::get_user_node($user_id);
$total_emails;
$total_emails_by_user;
$total_emails_by_user_valid;
$total_emails_valid;
$total_delivered;
$total_delivered_user;

//|| $user_id == 4060 removed by jpdamasco
$role_added_depts = array();
// if($user_id == 1252020 ) $node = px_h3_v3::get_department(128,1);
$sql = "SELECT h3_ids FROM role_details WHERE user_id = $user_id AND role_lkp_id IN (72,73,200,202) AND x = 'active' AND user_id != 5140";
$db->query($sql);
$result = $db->fetch_assoc();
if (!empty($result) && $result['h3_ids'] != 0) {
	$hid = array_filter(explode(",", $result['h3_ids']));
	$node = px_h3_v3::get_department($hid[0], 1);

	foreach($hid as $ht_id) $role_added_depts[$ht_id]=1;

}

$roles = array_merge(px_login::roles("ORG"), px_login::roles("QA"));
$is_TL = array_intersect(array(9, 39, 86, 35, 72, 73, 200, 202), $roles) || $user_id == 1251759;
$is_csm =  array_intersect(array(12), $roles);

//4060 added by jpdamasco || px_login::get_info('user_id') == 4060
if (in_array(183, $roles) || $user_id == 1248075 ) {
	
	
		$node = px_h3_v3::get_department($node['parent_id'], 1);	// not a csm/am but has a role to oversee his cluster's reports
		if (!preg_match('/cluster/i', $node['node'])) $node = px_h3_v3::get_department($node['parent_id'], 1);
	



	
}

if ($user_id == 57 || $user_id == 4060 || $user_id == 1241435) $node = px_h3_v3::get_department(7, 1);
if ($is_email_specialist) {
	//$node = px_h3_v3::get_department(120,1);
	//$include = ",$user_node[hierarchy_tree_id]";
	if ($node['node_type'] != 'group') {
		$node = px_h3_v3::get_department($node['hierarchy_tree_id'], 1);
		if ($node['hierarchy_tree_id'] == 163) {
			$node = px_h3_v3::get_department($node['parent_id'], 1);
		}
	}
}

$positive = $list_ids_per_client = $list_ids_per_agent = array();

$my_team  = px_h3_v3::get_users($node);
/*if($_SERVER['REMOTE_ADDR']=="192.168.60.195"){
    echo "<pre>";
    print_r($his_team);
    echo "</pre>";
}*/
if($user_id == 1241435){
    $node2_jxj = px_h3_v3::get_department(39, 1);
    $his_team = px_h3_v3::get_users($node2_jxj);
    if(count($his_team) != 0){
        $my_team = array_merge($my_team,$his_team);
    }
}if($user_id == 1244117){ //k6
    $node2_jxj = px_h3_v3::get_department(505, 1);
    $his_team = px_h3_v3::get_users($node2_jxj);
    if(count($his_team) != 0){
        $my_team = array_merge($my_team,$his_team);
    }
}if($user_id == 1248075){ //appleb
    $node2_jxj = px_h3_v3::get_department(505, 1);
    $his_team = px_h3_v3::get_users($node2_jxj);
    if(count($his_team) != 0){
        $my_team = array_merge($my_team,$his_team);
    }
}

if ($user_id == 1251893) { //rheat
	$node2_jxj = px_h3_v3::get_department(18, 1);
    $his_team = px_h3_v3::get_users($node2_jxj);
    if(count($his_team) != 0){
        $my_team = array_merge($my_team,$his_team);
    }
}


if($role_added_depts && in_array($user_id,array( 52483,1251579,1240374,1253226,1253234,4962 ))){ //carmi, julie morales, 1240374 kristia, 1253226 bradfordd, 1253234 Sara MT CSM of IAO, 4962 Melisa timbol
    foreach($role_added_depts as $ht_id => $null){
        $node2_jxj = px_h3_v3::get_department($ht_id, 1);
        $his_team = px_h3_v3::get_users($node2_jxj);
        if(count($his_team) != 0){
            $my_team = array_merge($my_team,$his_team);
        }

        if($ht_id == 505){ // colombia itps, then add colombia trainees
            $node2_jxj = px_h3_v3::get_department(515, 1);
            $his_team = px_h3_v3::get_users($node2_jxj);
            if(count($his_team) != 0){
                $my_team = array_merge($my_team,$his_team);
            }
            $node2_jxj = px_h3_v3::get_department(528, 1); //mozambique trainees
            $his_team = px_h3_v3::get_users($node2_jxj);
            if(count($his_team) != 0){
                $my_team = array_merge($my_team,$his_team);
            }
        }
    }
}

/* if ($user_id == 1251619) {
	$my_team[] = 1251990;
} */



$downline_h3_ids = px_h3_v3::get_downline_nodes($node);
if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') {
	// echo "<pre>" . print_r($downline_h3_ids, true) . "</pre>";
	// $downline_h3_ids[] = 520;
}

// echo print_r($downline_h3_ids, true);
/* $sql = "SELECT
		 h3_ids 
		FROM
		 role_details 
		WHERE
		 user_id = $user_id 
		AND
		 role_lkp_id IN (72,73,200,202)";

$db->query($sql);

while ($row = $db->fetch_assoc()) {
	$h3ids = explode(",", $row['h3_ids']);
	$downline_h3_ids = array_unique(array_merge($downline_h3_ids, $h3ids));
} */
// echo print_r($downline_h3_ids, true);
//remove DAVAO email specialists team
if (($key = array_search(191, $downline_h3_ids)) !== false) {
	unset($downline_h3_ids[$key]);
}
if ($user_id == 8626) $downline_h3_ids = get_hids($user_id);


if (!empty($node)) {
	$hquery_id[] = $node['hierarchy_tree_id'];

	if (in_array($user_id, array(1253415, 1252947))) // fernanda, juan manuel
		$hquery_id [] = 520;

	if (!empty($node['children'])) {
		unset($node['children'][191]); //remove DAVAO email specialists team
		foreach ($node['children'] as $hid => $n)
			$hquery_id[] = $hid;
	}

	$hquery = "Select * from hierarchy_tree_details where hierarchy_tree_id IN (" . implode(",", $hquery_id) . ")";
	if ($is_csm && $downline_h3_ids) $hquery .= " OR hierarchy_tree_id IN (" . implode(",", $downline_h3_ids) . ")";

	$db->query($hquery);

	while ($row = $db->fetch_assoc()) :
		if (!in_array($row['user_id'], $my_team)) $my_team[] = $row['user_id'];
	endwhile;


	// echo $hquery;
	if ($_GET['debug1']) lib::debug($my_team);
}
$active_team = array();
$active_sql = "SELECT * FROM hierarchy_tree_details htd
					INNER JOIN employees e USING(user_id)
					INNER JOIN users u USING(user_id)
					WHERE e.x = 'active' AND u.x = 'active' AND htd.x='active' AND htd.hierarchy_tree_id IN (" . implode(",", $hquery_id) . ")";

if ($is_csm && $downline_h3_ids) $active_sql .= " OR hierarchy_tree_id IN (" . implode(",", $downline_h3_ids) . ")";
$db->query($active_sql);

while ($row = $db->fetch_assoc()) :
	if (!in_array($row['user_id'], $active_team)) $active_team[] = $row['user_id'];
endwhile;

if (join(",", $downline_h3_ids) != '') {
	$sql = "SELECT * FROM `hierarchy_tree` WHERE `hierarchy_tree_id` 
	IN (" . join(",", $downline_h3_ids) . "{$include}) AND node_type ='group' AND `hierarchy_tree_id`  NOT IN (13,14) 
	AND node NOT LIKE '%Inbound%' AND node NOT LIKE '%cluster%' ";

	$db->query($sql);
	while ($row = $db->fetch_assoc()) :

		$groups[$row['hierarchy_tree_id']] = $row['node'];

	endwhile;
}

if ($user_id == 1241435) 	{
	$groups[17] = 'ILO CS: Voice Trainees';
	$groups[133] = 'On The Job Trainees Iloilo';
	$groups[273] = 'ILO CS: Seasonal-Based Trainees';
	$groups[280] = 'ILO CS : Non-Voice Trainees';
	$groups[282] = 'ILO Non-CS Trainees';
	$groups[458] = 'ILO Probationary';
}
if ($user_id == 1244117){ //k6 ITPS and colombia
    // $groups[11] = 'ILO CS: ITPS - Software';
    // $groups[505] = 'Colombia CS: ITPS';
}

//k6, julie, apple 
if(in_array($user_id, array(1244117,1251579,1248075))){
    $groups[505] = 'Colombia CS: ITPS';
    // $groups[515] = 'Colombia HR Trainees';

}
if ($_SERVER['REMOTE_ADDR'] == "192.168.60.172") {
	// echo "<pre>" . print_r($groups, true) . "</pre>";
}
if($role_added_depts && in_array($user_id,array( 52483,1251579,4962 ))) { //carmi , julie morales, 4962 Melisa Timbol
    $sql = "SELECT * FROM `hierarchy_tree` WHERE `hierarchy_tree_id` 
	IN (" . join(",", array_keys($role_added_depts)) . ") ";

    $db->query($sql);
    while ($row = $db->fetch_assoc()) :

        $groups[$row['hierarchy_tree_id']] = $row['node'];

    endwhile;

}
/* if ($_SERVER['REMOTE_ADDR'] = "192.168.84.38")
	return lib::debug($groups, 1); */

if (count($groups) > 1) {
	foreach ($groups as $gid => $grp) :

		$str = implode(',', $downline_h3_ids);
		$groups[$str] = 'All';

		if ($_POST['bus_group']) $selected_group = $_POST['bus_group'];
		else  $selected_group = $str;
		//$downline_h3_ids=array_keys($downline_h3_ids);
		//$downline_h3_ids = array($gid);
		break;
	endforeach;

}
if (!empty($_POST['bus_group'])) {
    if ($_SERVER['REMOTE_ADDR'] == '192.168.60.173') {
        echo '<pre> POST bus_group' . "{$_POST['bus_group']}" . ' </pre>'. "<br />";
    }
	$downline_h3_ids = explode(',', $_POST['bus_group']);

	/*if(preg_match('/cluster/i',$groups[$_POST['bus_group']])){
			$sql = "SELECT * FROM `hierarchy_tree` WHERE `parent_id`  = ".$_POST['bus_group']."
			AND node_type ='group' ";
			//echo $sql;
	   	    $db->query($sql);
			  
		while ($row = $db->fetch_assoc()):
	
			$downline_h3_ids[] = $row['hierarchy_tree_id'];
			
		endwhile;	
		
	}
	else $downline_h3_ids=array($_POST['bus_group']);*/
}
if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') { //jp
	// echo $_SERVER['REMOTE_ADDR'];
	// echo "<pre>";
	// print_r($groups);
	// echo "</pre>";
	/* echo "<pre>";
	print_r($downline_h3_ids);
	echo "</pre>"; */

}
// echo print_r($selected_group, true);

$dm_states = px_report::$dm_states;

#if (array_intersect(array(93, 13, 69, 16, 90, 91, 21, 64, 65, 123, 68),$downline_h3_ids)) $apac = 1;

$tz = isset($_POST['timezone']) ? $_POST['timezone'] : 'US/Pacific';
$billsec = isset($_POST['billsec']) ? $_POST['billsec'] : '15';

if ($tz == 'Asia/Manila') {
	$apac = 1;
	$tzid = 3;
} else {
	$apac = 0;
	$tzid = 10;
}


if (!empty($downline_h3_ids)) {
	$hids = implode(',', $downline_h3_ids);
	$user_node = px_h3_v3::get_user_node($user_id);

	if ($is_TL && !$_POST['bus_group']) {
		$dept_node = px_h3_v3::get_department($user_node['hierarchy_tree_id']);
		$dept_id = $dept_node['hierarchy_tree_id'];
		$hids .= ", $dept_id";

        if ($_SERVER['REMOTE_ADDR'] == '192.168.60.173') {
            echo '<pre> HIDS' . print_r($dept_node, true) . ' </pre>'. "HIDS {$hids}";
        }
	}
}


// echo print_r($node, true);

$bus_id = $node['hierarchy_tree_id'];
$now = new px_date_time();
$nowx = new px_date_time();
$now = $now->offset(-12);
$nowx = $nowx->offset(8);

$agent_total_num = 0;
$all_calls_agents = 0;
$all_tagged_calls_agents  = 0;
$total_hours_agents = 0;

if (empty($_POST['from'])) {
	$dfrom = $now->dt('Y-m-d');
	$dto = $now->dt('Y-m-d');
	if ($apac) {
		$dfrom = $nowx->dt('Y-m-d');
		$dto = $nowx->dt('Y-m-d');
	}
} elseif (!empty($_POST['from'])) {
	$dfrom = $_POST['from']; //'2011-04-11';
	$dto = $_POST['to']; //'2011-04-11';
	$_SESSION['tldshb-dt-from'] = $dfrom;
	$_SESSION['tldshb-dt-to'] = $dto;
}

if (empty($_POST['from_time'])) {
	$dfrom_time = "00:00";
	$dto_time = "23:59";
	if ($apac) {
		$dfrom_time = "08:00";
		$dto_time = "07:59";
	}
} elseif (!empty($_POST['from_time'])) {
	$dfrom_time = $_POST['from_time']; //'2011-04-11';
	$dto_time = $_POST['to_time']; //'2011-04-11';
	$_SESSION['tldshb-dt-from_time'] = $dfrom_time;
	$_SESSION['tldshb-dt-to_time'] = $dto_time;
}

$from_time = "{$dfrom_time}:00";
$to_time = "{$dto_time}:59";

$from = "{$dfrom} {$from_time}";
$to = "{$dto} {$to_time}";
if (in_array(43, explode(",", $hids)) || in_array(86, explode(",", $hids))) $hids .= ",37";
if (in_array(218, explode(",", $hids)) || in_array(124, explode(",", $hids))) $hids .= ",43";
/* if ($user_id == 1253415)
	$hids .= ",505"; */
$hid_q = "hierarchy_tree_id IN ({$hids})";
if ($user_id == 1250448)  $hid_q .= " OR client_id IN (20944) ";
if ($user_id == 1244858)  $hid_q .= " OR client_id IN (21436) "; //markja include tenet consulting
/*if ($apac)$tz = "Asia/Manila";
else $tz = "US/Pacific";*/
//$tz = "UTC";//"Asia/Manila";//
if ($_REQUEST['test'] == 2) $test = 2;
$debug = $_REQUEST['debug'];

//&& $user_id != 4060 - removed by jp damasco
if ($user_id != 57) $exclude_callbox_cs = "";
else $exclude_callbox_cs = " AND client_id != 11404 ";

if ($apac) {
	$filter = "`date_contacted` BETWEEN date(CONVERT_TZ('$from', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$to','{$tz}', 'UTC' ))";
} else $filter = "(date_contacted BETWEEN date(CONVERT_TZ('$from', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$to','{$tz}', 'UTC' )))";
$dataprofilo_date_from = date("Y-m-d", strtotime("-9 days"));
if ($user_id == 1236545) $filter = "(date_contacted BETWEEN '{$dataprofilo_date_from} 00:00:00' AND '$to') ";


$read_db = new MySQLi(config::get_server_by_name("main"), "app_pipe", "a33-pipe", "callbox_pipeline2");
// echo $hid_q;
$sql2 = "SELECT distinct
		  client_id, company , hierarchy_tree_id
		FROM 
		  _vw_dr_ob_clients_per_group
		WHERE 
		  ($hid_q) {$exclude_callbox_cs} AND 
		  ({$filter})
		  AND company not like 'Demo Company'
		ORDER BY
		  company";


// echo $sql2;

$rpt = $read_db->query($sql2);

while ($row = $rpt->fetch_assoc()) :
	if (!in_array($dept_node['hierarchy_tree_id'], array(43, 124)) && substr($row['company'], 0, 7) == "Callbox" && $user_id != 1236545) continue;
	$clients[$row['hierarchy_tree_id']][$row['client_id']]['company'] = $row['company'];
	$clients_ids[$row['client_id']] = $row['company'];
endwhile;
if (!$is_TL && !$is_PL) {
	echo " You are not a Leader.  Please contact the programmers to check your roles.";
	exit;
}

if (empty($clients_ids)) {
	$sql2 = "SELECT distinct
				 client_id, company , hierarchy_tree_id
				FROM 
				 _vw_dr_ob_clients_per_group
				WHERE
				 ({$filter})
				AND company not like 'Demo Company'
				ORDER BY
				company";
				$rpt = $read_db->query($sql2);

	while ($row = $rpt->fetch_assoc()) :
		$all_clients[$row['client_id']] = $row['company'];
	endwhile;
}

if (empty($clients)) {
	echo "No clients found. ";
	if (!$is_TL && !$is_PL) {
		echo "You're not allowed here";
		exit;
	}
}
if ($user_id == 1236545) {
	$rpt = $read_db->query("SELECT distinct
		  client_id, company , hierarchy_tree_id
		FROM 
		  _vw_dr_ob_clients_per_group
		WHERE 
		  company ='Acer Sales & Services Sdn. Bhd.'
		  AND company not like 'Demo Company'
		ORDER BY
		  company");
	while ($row = $rpt->fetch_assoc()) :
		$clients[$row['hierarchy_tree_id']][$row['client_id']]['company'] = $row['company'];
		$clients_ids[$row['client_id']] = $row['company'];
	endwhile;
}

if ($debug) echo $sql2;
/*$db->query(
	"SELECT *
	FROM `events_tm_ob_lkp`
	INNER JOIN `events_tm_ob_txn` USING (`event_tm_ob_lkp_id`)
	INNER JOIN `call_records` USING (`call_record_id`)
	INNER JOIN `event_states_lkp` USING (`event_state_lkp_id`)
	INNER JOIN `client_lists` USING (`client_list_id`)
	INNER JOIN `client_job_orders` USING (`client_job_order_id`)
	INNER JOIN `client_accounts` USING (`client_account_id`)
	INNER JOIN `clients` c USING (`client_id`)
	INNER JOIN `target_details` td ON td.`target_detail_id` = c.`target_detail_id`
	INNER JOIN `comp_details` USING (`comp_detail_id`)
	INNER JOIN `companies` USING (`company_id`)
	WHERE `user_id` = 51587 AND `date_contacted` BETWEEN '$dfrom' AND '$dto' "
);*/
//WHERE `user_id` = 51587 AND `date_contacted` >= '{$now->dt('Y-m-d')}' AND CONCAT_WS(' ', `date_contacted`, `time_contacted`) >= '{$now->dt()}'"
//$clients = array(); 


$accts = array();
$succ_calls = array();
$total_calls = 0;
$total_succ_calls = 0;
$total_manhours = 0;

$teams = array();
$nodes = array();

if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') {
	// echo '<pre>' . print_r($my_team, true) . '</pre>';
}

if (!empty($clients) || !empty($my_team)) :
	if (!empty($clients_ids)) {
		$client_ids = join(",", array_keys($clients_ids));
		$c_filter = " AND client_id IN ($client_ids) ";
	}

	/******************** Jan 26, 2012  removed the $my_team filter because they want to see calls of resigned agents or transferred agents **************/

	//if($node['parent_id'] == 37 || preg_match('/sales/i',$node['node']) || $user_id == 57) {
	$user_filter = implode(",", $my_team);
	$my_tms = " AND events_tm_ob_lkp.user_id in ($user_filter)";
	//}
	$main_filter = $c_filter;

	if ($user_id == 1236545) {
		$ilo_agents = "and e.`op_center_lkp_id` = 1";
		$main_filter = $my_tms;
	}elseif ($user_id == 1244117 || $user_id == 1248075) { //k6 and apple grace bantiquete to check colombia employees
		$ilo_agents = "";
		$main_filter = " AND (client_id IN ($client_ids) OR e.branch='colombia') ";
        if ($_SERVER['REMOTE_ADDR'] == '192.168.60.173') {
            echo '<pre>' . print_r($client_ids, true) . '</pre>';
        }
        /*if ($_REQUEST['test'] == 2) :
            $h3_id = px_h3_v3::get_user_node(1252969);
            if (empty($h3_id)) {
                $parent = getHierarchyID(1252969, true);
                $h3_id['parent_id'] = $parent;
            }
            $org = px_h3_v3::get_department($h3_id['parent_id'], 1);
            if (!in_array($org['hierarchy_tree_id'], $agent_dept)) {
               echo "ouch ".$org['hierarchy_tree_id']. " " . print_r($agent_dept,true);
            }
            echo "__________________";
                print_r($agent_dept);
            exit;
        endif;*/

	} else $ilo_agents = "";

	$sql_query = "SELECT htd.hierarchy_tree_id AS team_id, ht.node AS team, `client_id`,`list`, e.`user_id`, e.`first_name`, e.`last_name`, `success_event_state_ids`, `event_state_lkp_id`, `event_state`, state_group, client_accounts.client_account_id, account_number, client_accounts.hierarchy_tree_id,client_list_detail_id,
		 DATE(CONVERT_TZ(CONCAT_WS(' ', `date_contacted`, `time_contacted`), 'UTC' ,'{$tz}' )) as dcontacted, duration, events_tm_ob_txn.notes, events_tm_ob_txn.event_tm_ob_txn_id, client_lists.client_list_id,
		 `client_accounts`.x as account_x, client_lists.x as list_x,
		 e.extension as ext, client_job_orders.dm_profile_completed as dmpc, billsec,
		 IF(lead_channel_src.channel = '' OR lead_channel_src.channel IS NULL,ch.channel,lead_channel_src.channel) as channel, events_tm_ob_txn.channel_lkp_id
		 FROM `events_tm_ob_lkp`
		 INNER JOIN `events_tm_ob_txn` USING (`event_tm_ob_lkp_id`)
		 INNER JOIN `call_records` USING (`call_record_id`)
		 INNER JOIN `event_states_lkp` USING (`event_state_lkp_id`)
		 INNER JOIN `client_lists` USING (`client_list_id`)
		 INNER JOIN `client_job_orders` USING (`client_job_order_id`)
		 INNER JOIN `client_accounts` USING (`client_account_id`)
		 INNER JOIN `employees` e USING (`user_id`)
		 INNER JOIN hierarchy_tree_details htd ON htd.user_id = e.user_id AND htd.x = 'active'
		 INNER JOIN hierarchy_tree ht ON ht.hierarchy_tree_id = htd.hierarchy_tree_id
		 LEFT OUTER JOIN lead_channel_src ON (events_tm_ob_txn.event_tm_ob_txn_id = lead_channel_src.event_tm_ob_txn_id)
		 LEFT OUTER JOIN channels_lkp ch ON (events_tm_ob_txn.channel_lkp_id = ch.channel_lkp_id)
		 WHERE event_state_lkp_id != 83 and `date_contacted` BETWEEN date(CONVERT_TZ('$from', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$to','{$tz}', 'UTC' )) 
		 AND CONCAT_WS(' ', `date_contacted`, `time_contacted`) BETWEEN CONVERT_TZ('$from', '{$tz}', 'UTC' ) AND CONVERT_TZ('$to','{$tz}', 'UTC' )
		 /* AND (billsec >= 5 OR event_state_lkp_id IN(18,19,25,645) OR events_tm_ob_txn.channel_lkp_id > 1) */
		 AND events_tm_ob_txn.x='active' {$main_filter} {$ilo_agents}
		";
    if ($_SERVER['REMOTE_ADDR'] == '192.168.60.113') {

        echo $sql_query;
    }
	$db->query(
		$sql_query
	);


	if ($_REQUEST['test'] == 2) :

		echo '<pre> <br /><br /><br /><br /><br /><br /><br />';

		echo "SELECT `client_id`,`list`, e.`user_id`, e.`first_name`, e.`last_name`, `success_event_state_ids`, `event_state_lkp_id`, `event_state`, state_group, client_accounts.client_account_id, account_number, client_accounts.hierarchy_tree_id,client_list_detail_id,
		 DATE(CONVERT_TZ(CONCAT_WS(' ', `date_contacted`, `time_contacted`), 'UTC' ,'{$tz}' )) as dcontacted, duration, events_tm_ob_txn.notes, events_tm_ob_txn.event_tm_ob_txn_id, client_lists.client_list_id,
		 `client_accounts`.x as account_x, client_lists.x as list_x,
		 e.extension as ext, client_job_orders.dm_profile_completed as dmpc, billsec,
		 IF(lead_channel_src.channel = '' OR lead_channel_src.channel IS NULL,ch.channel,lead_channel_src.channel) as channel, events_tm_ob_txn.channel_lkp_id
		 FROM `events_tm_ob_lkp`
		 INNER JOIN `events_tm_ob_txn` USING (`event_tm_ob_lkp_id`)
		 INNER JOIN `call_records` USING (`call_record_id`)
		 INNER JOIN `event_states_lkp` USING (`event_state_lkp_id`)
		 INNER JOIN `client_lists` USING (`client_list_id`)
		 INNER JOIN `client_job_orders` USING (`client_job_order_id`)
		 INNER JOIN `client_accounts` USING (`client_account_id`)
		 INNER JOIN `employees` e USING (`user_id`)		 
		 LEFT OUTER JOIN lead_channel_src ON (events_tm_ob_txn.event_tm_ob_txn_id = lead_channel_src.event_tm_ob_txn_id)
		 LEFT OUTER JOIN channels_lkp ch ON (events_tm_ob_txn.channel_lkp_id = ch.channel_lkp_id)
		 WHERE event_state_lkp_id != 83 and `date_contacted` BETWEEN date(CONVERT_TZ('$from', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$to','{$tz}', 'UTC' )) 
		 AND CONCAT_WS(' ', `date_contacted`, `time_contacted`) BETWEEN CONVERT_TZ('$from', '{$tz}', 'UTC' ) AND CONVERT_TZ('$to','{$tz}', 'UTC' )
		 AND (billsec >= 5 OR event_state_lkp_id IN(18,19,25,645) OR events_tm_ob_txn.channel_lkp_id > 1)
		 AND events_tm_ob_txn.x='active' {$main_filter} {$ilo_agents}";
		echo '</pre>';

	endif;

	$agent_calls = array();
	$agent_calls2 = array();
	$agent_calls_details = array();
	$agent_array =  array();
	$per_day_count = array();
	$dm_calls = array();
	$dm_notes = array();
	//$dm_notes_txn = array();
	$dm_notes_c = array();
	$dm_invalid = array();
	$actions = array();
	##
	# MIO Oct 30, 2012
	$list_array = array();
	$less_five_talk_time = array();
	#
	##

	/* if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') {
		echo "SELECT `client_id`,`list`, e.`user_id`, e.`first_name`, e.`last_name`, `success_event_state_ids`, `event_state_lkp_id`, `event_state`, state_group, client_accounts.client_account_id, account_number, client_accounts.hierarchy_tree_id,client_list_detail_id,
		DATE(CONVERT_TZ(CONCAT_WS(' ', `date_contacted`, `time_contacted`), 'UTC' ,'{$tz}' )) as dcontacted, duration, events_tm_ob_txn.notes, events_tm_ob_txn.event_tm_ob_txn_id, client_lists.client_list_id,
		`client_accounts`.x as account_x, client_lists.x as list_x,
		e.extension as ext, client_job_orders.dm_profile_completed as dmpc, billsec,
		IF(lead_channel_src.channel = '' OR lead_channel_src.channel IS NULL,ch.channel,lead_channel_src.channel) as channel, events_tm_ob_txn.channel_lkp_id
		FROM `events_tm_ob_lkp`
		INNER JOIN `events_tm_ob_txn` USING (`event_tm_ob_lkp_id`)
		INNER JOIN `call_records` USING (`call_record_id`)
		INNER JOIN `event_states_lkp` USING (`event_state_lkp_id`)
		INNER JOIN `client_lists` USING (`client_list_id`)
		INNER JOIN `client_job_orders` USING (`client_job_order_id`)
		INNER JOIN `client_accounts` USING (`client_account_id`)
		INNER JOIN `employees` e USING (`user_id`)
		LEFT OUTER JOIN lead_channel_src ON (events_tm_ob_txn.event_tm_ob_txn_id = lead_channel_src.event_tm_ob_txn_id)
		LEFT OUTER JOIN channels_lkp ch ON (events_tm_ob_txn.channel_lkp_id = ch.channel_lkp_id)
		WHERE event_state_lkp_id != 83 and `date_contacted` BETWEEN date(CONVERT_TZ('$from', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$to','{$tz}', 'UTC' )) 
		AND CONCAT_WS(' ', `date_contacted`, `time_contacted`) BETWEEN CONVERT_TZ('$from', '{$tz}', 'UTC' ) AND CONVERT_TZ('$to','{$tz}', 'UTC' )
		AND (billsec >= 5 OR event_state_lkp_id IN(18,19,25,645) OR events_tm_ob_txn.channel_lkp_id > 1)
		AND events_tm_ob_txn.x='active' {$main_filter} {$ilo_agents}";
	} */

	while ($row = $db->fetch_assoc()) :

		/*	if($_REQUEST['test'] == 2){						
		 	if($client_ids[$row['client_id']] == "") continue;
		}*/		
		
		if ($row['billsec'] >= 5 || in_array($row['event_state_lkp_id'], array(18, 19, 25, 645)) || $row['channel_lkp_id'] > 1) {
			$nodes[$row['team_id']] = $row['team'];
			$teams[$row['team_id']][$row['user_id']]++;
			/*if(!in_array($row['user_id'],$my_team)) { 
				
				$calls_of_tms_not_in_dept[$row['first_name'] . " " . $row['last_name']]++;
				$calls_of_tms_not_in_depts[$row['first_name'] . " " . $row['last_name']][$clients_ids[$row['client_id']]][$row['list']]++;
				continue;
			}*/
			//$htreeclients[$row['hierarchy_tree_id']]['calls']++;
			//$total_calls++; // - is computed at summary.php

			//if($row["dmpc"] == "no" && $row['event_state_lkp_id'] == "589")
			//continue;

			if ($row['account_x'] == 'active') {
				$agent_calls[$row['client_id']][$row['client_account_id']][$row['user_id']]['count']++;
				$agent_calls[$row['client_id']][$row['client_account_id']][$row['user_id']]['name'] = htmlentities($row['first_name'] . " " . $row['last_name']);
				$brkdown[$row['client_id']][$row['user_id']]++;
			}

			$agent_array[$row['user_id']]['name'] = htmlentities($row['first_name'] . " " . $row['last_name']);
			$agent_array[$row['user_id']]['calls']++;
			$ch = $row['channel'] == 'Calling' ? 'Voice' : $row['channel'];
			if ($ch == 'Voice') {
				$actions[$row['user_id']]['channels'][$ch]['count']++;
			} else {
				$actions[$row['user_id']]['channels']['Non-voice']['count']++;
				$actions[$row['user_id']]['channels']['Non-voice']['channel'][$ch]++;
			}
			$actions[$row['user_id']]['channels']['total']++;

			$actions[$row['user_id']]['name'] = htmlentities($row['first_name'] . " " . $row['last_name']);
			if ($row['billsec'] >= 5 || (in_array($row['event_state_lkp_id'], array(18, 19, 25, 645)) && $row['channel_lkp_id'] == 1)) {
				$agent_array[$row['user_id']]['answered_calls']++;
			}
			$agent_array[$row['user_id']]['ext'] = $row["ext"];
			if ($row['state_group'] == 'Positive Contact') {
				$agent_array[$row['user_id']][$row['state_group']]++;
				$positive[$row['user_id']][$row['event_state']]++;
			}

			if ($row['event_state_lkp_id'] == 589) {
				$agent_array[$row['user_id']][$row['event_state']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']][$row['event_state']]++;
			}


			if ($row['event_state_lkp_id'] == 589) {
				$agent_array[$row['user_id']][$row['event_state']]++;
				$agent_array[$row['user_id']]['prof_completed_per_client'][$row['client_id']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']][$row['event_state']]++;
			}

			//added by jp damasco as instructed by miss josette
			$list_ids_per_client[$row['client_id']][$row['client_list_id']]++;
			$list_ids_per_agent[$row['user_id']][$row['client_list_id']]++;

			if ($row['event_state_lkp_id'] == 19) {
				$agent_array[$row['user_id']]['appointment']++;
				$agent_array[$row['user_id']]['appointment_per_client'][$row['client_id']]++;
				$agent_array[$row['user_id']]['appt_lists_per_client'][$row['client_id']][$row['client_list_id']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']]['appointment']++;
			}

			if ($row['event_state_lkp_id'] == 18) {
				$agent_array[$row['user_id']]['mql']++;
				$agent_array[$row['user_id']]['mql_per_client'][$row['client_id']]++;
				$agent_array[$row['user_id']]['mql_lists_per_client'][$row['client_id']][$row['client_list_id']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']]['mql']++;
			}
			if ($row['event_state_lkp_id'] == 589) {
				$agent_array[$row['user_id']]['profile_completed']++;
				$agent_array[$row['user_id']]['profile_completed_per_client'][$row['client_id']]++;
				$agent_array[$row['user_id']]['profile_completed_lists_per_client'][$row['client_id']][$row['client_list_id']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']]['profile_completed']++;
			}
			if ($row['event_state_lkp_id'] == 645) {
				$agent_array[$row['user_id']]['webinar']++;
				$agent_array[$row['user_id']]['webinar_per_client'][$row['client_id']]++;
				$agent_array[$row['user_id']]['webinar_lists_per_client'][$row['client_id']][$row['client_list_id']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']]['webinar']++;
			}
			if ($row['event_state_lkp_id'] == 35) {
				$agent_array[$row['user_id']]['for_quali']++;
				$agent_array[$row['user_id']]['for_quali_per_client'][$row['client_id']]++;
				$agent_array[$row['user_id']]['for_quali_lists_per_client'][$row['client_id']][$row['client_list_id']]++;
				$clients[$row['hierarchy_tree_id']][$row['client_id']]['for_quali']++;
			}
			$agent_array[$row['user_id']]['lists_called'][$row['client_id']][$row['client_list_id']]++;
			// if ($row['event_state_lkp_id'] == 19) {
			// 	$agent_array[$row['user_id']]['appointment']++;
			// 	$agent_array[$row['user_id']]['appointment_per_client'][$row['client_id']]++;
			// 	$clients[$row['hierarchy_tree_id']][$row['client_id']]['appointment']++;
			// }

			// if ($row['event_state_lkp_id'] == 18) {
			// 	$agent_array[$row['user_id']]['mql']++;
			// 	$agent_array[$row['user_id']]['mql_per_client'][$row['client_id']]++;
			// 	$clients[$row['hierarchy_tree_id']][$row['client_id']]['mql']++;
			// }
			// if ($row['event_state_lkp_id'] == 645) {
			// 	$agent_array[$row['user_id']]['webinar']++;
			// 	$agent_array[$row['user_id']]['webinar_per_client'][$row['client_id']]++;
			// 	$clients[$row['hierarchy_tree_id']][$row['client_id']]['webinar']++;
			// }
			//end row by jp damasco

			if ($row['account_x'] == 'active') {
				$list_array[$row['client_account_id']][$row['list']]['count']++;
				$list_names[$row['client_list_id']] = $row['list'];

				$clients[$row['hierarchy_tree_id']][$row['client_id']]['count']++;
				$accts[$row['client_account_id']] = $row['account_number'];
			}

			$per_day_count[$row['dcontacted']]++;

			if (!empty($row['success_event_state_ids'])) $success_def = explode(",", $row['success_event_state_ids'] . ",35");
			else $success_def = array(35);

			if (in_array($row['event_state_lkp_id'], $success_def)) :
				if ($row['event_state'] == 'Lead Completed')	$row['event_state'] = "Lead";
				elseif (preg_match("/quali/i", $row['event_state']))	$row['event_state'] = "for Quali";
				elseif ($row['event_state'] == 'Appointment Set')	$row['event_state'] = "Appt. Set";
				$agent_calls[$row['client_id']][$row['client_account_id']][$row['user_id']]['succ'][$row['event_state']]++;


				if ($row['event_state'] != 'Lead/Appt for Qualification') {
					//$succ_calls_new[$row['client_id']][$row['account_number']][$clients_ids[$row['client_id']]]++;
					$succ_calls['client'][$clients_ids[$row['client_id']]]++;
					$succ_calls['agent'][htmlentities($row['first_name'] . " " . $row['last_name'])]++;
					$agent_array[$row['user_id']]['succ']++;
					$total_succ_calls++;
				}
			endif;
			if ($row['event_state_lkp_id'] == 149 || $row['event_state_lkp_id'] == 16) $agent_array[$row['user_id']]['left_vm']++;
			#dm_reached##  a record is counted as dm reached once only per user per day 
			if (
				in_array($row['event_state_lkp_id'], $dm_states)
				|| ($row["dmpc"] == "yes" && in_array($row['event_state_lkp_id'], array(589, 271)))
			) :
				if ($row['duration']) { // || (in_array($row['client_id'],array(17891,15011)) && $row['event_state_lkp_id'] == 271)
					$dm_calls['duration_ids'][$row['user_id']][$row['client_list_detail_id']]++;
					$dm_calls['duration'][$row['user_id']][] = $row['duration'];
				}
				if (clean($row['notes']) == "") {
					$dm_invalid['no_notes'][$row['user_id']][$row['client_list_detail_id']]++;
				}
			endif;

			if ((in_array($row['event_state_lkp_id'], $dm_states) || ($row["dmpc"] == "yes" && in_array($row['event_state_lkp_id'], array(589, 271)))
			)  && $row['notes'] != "") :
				$proceed = 1;

				$dm_calls['count'][$row['user_id']]++; 													//dm reached calls

				if (empty($dm_notes[$row['user_id']][$row['client_list_detail_id']])) {
					$dm_notes[$row['user_id']][$row['client_list_detail_id']][] = clean($row['notes']);
					$dm_notes_c[$row['user_id']][$row['client_list_detail_id']]++;				//dm reached calls w/ unque notes
				} else {
					$notes = $dm_notes[$row['user_id']][$row['client_list_detail_id']];
					$clean = clean($row['notes']);
					if (in_array($clean, $notes)) {
						$proceed = 0;
						$dm_invalid['repeated_notes'][$row['user_id']]++;
						
					} else {
						$dm_notes[$row['user_id']][$row['client_list_detail_id']][] = clean($row['notes']);
						$dm_notes_c[$row['user_id']][$row['client_list_detail_id']]++;
					}
				}
				/* if ($row['user_id'] == 1252009) {
					echo $row['client_list_detail_id'] . "<br/>";
				} */
				if ($proceed) {
					$dm_r[$row['client_id']][$row['client_account_id']][$row['user_id']][$row['dcontacted']][] = $row['client_list_detail_id'];
					$dmr_evt_states['agent'][$row['user_id']][$row['client_list_detail_id']][$row['event_state']]++;
					$state_group = ($row['state_group'] == 'Positive Contact' || $row['state_group'] == 'Negative Contact') ? $row['state_group'] : '';
					if ($row['event_state_lkp_id'] == 626) $state_group = 'Negative Contact';
					$channel_actions['agent'][$row['user_id']][$row['client_list_detail_id']]['engagements'][$ch][$state_group]['events'][$row['event_state']]++;
					$channel_actions['agent'][$row['user_id']][$row['client_list_detail_id']]['engagements'][$ch][$state_group]['count']++;
					// $channel_actions['agent'][$row['user_id']][$row['client_list_detail_id']]['engagements']['total']++;
					$dm_reached_total_1[$row['client_id']]++;
					//$dm_notes_txn[$row['user_id']][$row['client_list_detail_id']][$row['event_tm_ob_txn_id']]=1;
					$check_for_residential[$row['client_id']][] = $row['client_list_detail_id'];

					if ($row['state_group'] == 'Positive Contact') $dm_calls['positive'][$row['user_id']][$row['client_list_detail_id']]++;
				}
			endif;
			#dm_reached## computation is continued below (#dm_reached continuation##)

			if ($row['account_x'] == 'active') {
				$acct_hid[$row['client_account_id']] = $row['hierarchy_tree_id'];
			}

			if ($row['account_x'] != 'active') {
				if (isset($clients_ids[$row['client_id']])) unset($clients_ids[$row['client_id']]);

				if (isset($clients[$row['hierarchy_tree_id']][$row['client_id']])) unset($clients[$row['hierarchy_tree_id']][$row['client_id']]);
			}
		} else if ($row['billsec'] < 5 && $row['channel_lkp_id']== 1) {
			$less_five_talk_time[$row['user_id']]++;
		}

	endwhile;
	if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') {
		// echo '<pre>' . print_r($teams, true) . '</pre>';
	}
// include manual calls //

/*$db->query("SELECT ca.client_id, e.`user_id`, e.`first_name`, e.`last_name`, ca.hierarchy_tree_id,
			   ca.client_account_id, account_number FROM events_tm_manual_calls 
			   INNER JOIN client_accounts ca USING (client_account_id)
				INNER JOIN `employees` e USING (`user_id`)
				WHERE client_id IN ($client_ids) AND datetime_called BETWEEN CONVERT_TZ('$from', '{$tz}', 'UTC' ) 
				AND CONVERT_TZ('$to','{$tz}', 'UTC' ) AND billsec > 15");
	
	while($row = $db->fetch_assoc()){
		$agent_calls[$row['client_id']][$row['client_account_id']][$row['user_id']]['count']++;
		$agent_calls[$row['client_id']][$row['client_account_id']][$row['user_id']]['name'] = htmlentities($row['first_name'] . " " . $row['last_name']);
		$clients[$row['client_id']]['count']++;
		$accts[$row['client_account_id']] = $row['account_number'];
		$acct_hid[$row['client_account_id']] = $row['hierarchy_tree_id'];
		echo $row['account_number'].$row['last_name']."<br />";
	}*/


// --- manual calls //
//lib::debug($agent_calls);
endif;


#dm_reached continuation##
if (!empty($check_for_residential)) :
	$is_residential = array();
	foreach ($check_for_residential as $clientid => $ids) :

		$ids2 = array_unique($ids);
		$cdids = array_chunk($ids2, 10);
		$detail_ids = join(",", $cdids[0]);
		//check the first 10 detail ids if blank company then client is a residential campaign
		$sql = "select company from  client_list_details 
		inner join 	target_details  using (target_detail_id)
		inner join 	comp_details using (comp_detail_id)
		inner join 	companies using (company_id)
		where client_list_detail_id in ($detail_ids)";
		$db->query($sql);
		while ($row = $db->fetch_array()) {
			$company = $row[0];
		}

		if ($company) $is_residential[$clientid] = 0;
		else $is_residential[$clientid] = 1;   //company is blank so it's a residential

	endforeach;
endif;

$engagesment_breakdown = array();
// $dmr_evt_states['agent'][$row['user_id']][$row['client_list_detail_id']][$row['event_state']]++;
$dm_reached_total = 0;
foreach ($dm_r as $iclient => $a) {
	foreach ($a as $iacct => $b)
		foreach ($b as $user_id => $dates)
			foreach ($dates as $cdet_ids) {
				if ($is_residential[$iclient]) {
					//for residential	
					$cdet_ids_str = implode(",", $cdet_ids);
					$sql = "SELECT client_list_details.client_list_detail_id, contacts.* FROM client_list_details 
						INNER JOIN 	target_details  USING (target_detail_id)
						LEFT OUTER JOIN contacts USING (contact_id)
						WHERE client_list_detail_id IN ($cdet_ids_str) GROUP BY client_list_details.target_detail_id";
					$db->query($sql);
					/* if ($user_id == 1252009) {
						echo $sql . "<br/>";
					} */
					while ($row = $db->fetch_assoc()) {
						//checking for fname || lname if  residential
						/*$fname = trim($row['first_name']);
						$lname = trim($row['last_name']);
						if( strlen($fname) < 2 ||  strlen($lname) < 2 ){
							$invalid_names[$user_id][$row['client_list_detail_id']] = $row;
							continue;
						}	*/
						//removed checking for fname || lname if residential on 5/27/2019 - accdg to ms. aijeleth if not interested theyre not able to get the name					
						$dm_reached_total++;
						$dm_reached['agent'][$iclient][$iacct][$user_id]++;
						$dm_reached['client'][$iclient]++;
						$dm_rt[$iclient][$iacct][$user_id][] = $row['client_list_detail_id'];
						$agent_array[$user_id]['dm_reached']++;
						$dm_reached_brkdown[$user_id][$row['client_list_detail_id']]++;
						$engagesment_breakdown['agent'][$user_id]['engagements'][$row['client_list_detail_id']] = $channel_actions['agent'][$user_id][$row['client_list_detail_id']]['engagements'];
						$engagesment_breakdown['agent'][$user_id]['actions'] = $actions[$user_id]['channels'];
						$engagesment_breakdown['agent'][$user_id]['name'] = $actions[$user_id]['name'];
						
					}
				} else {
					//for b2b				
					$cdet_ids_str = implode(",", $cdet_ids);
					$sql = "SELECT * FROM client_list_details cld 				
						INNER JOIN target_info_details tid ON (cld.target_detail_id = tid.target_detail_id)
						INNER JOIN target_info_lkp til ON (tid.param=til.target_info_lkp_id)
						WHERE tid.info_type_lkp_id IN (11,60) AND client_list_detail_id IN ($cdet_ids_str) 
						AND info_value != '' GROUP BY tid.target_detail_id";
					//if($iclient == 12292) echo $sql."<br />";
					$db->query($sql);
					/* if ($user_id == 1252009) {
						echo $sql . "<br/>";
					} */
					while ($row = $db->fetch_assoc()) {
						$dm_reached_total++;
						$dm_reached['agent'][$iclient][$iacct][$user_id]++;
						$dm_reached['client'][$iclient]++;
						$dm_rt[$iclient][$iacct][$user_id][] = $row['client_list_detail_id'];
						$agent_array[$user_id]['dm_reached']++;

						$dm_reached_brkdown[$user_id][$row['client_list_detail_id']]++;
						$engagesment_breakdown['agent'][$user_id]['engagements'][$row['client_list_detail_id']] = $channel_actions['agent'][$user_id][$row['client_list_detail_id']]['engagements'];
						$engagesment_breakdown['agent'][$user_id]['actions'] = $actions[$user_id]['channels'];
						$engagesment_breakdown['agent'][$user_id]['name'] = $actions[$user_id]['name'];
						/* if ($user_id == 1252009) {
							echo $row['client_list_detail_id'] . "<br/>";
						} */
					}
				}
			}
}

//if(px_login::get_info('user_id') ==1244117) lib::debug($engagesment_breakdown);
// if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') {
// 	echo '<pre>' . print_r($agent_array,true) . '</pre>';
// }
/*foreach($dm_notes_c as $dm_uid => $dm_cldids){
	if($dm_uid != 1240381) continue;
	$dm_cldids = array_keys($dm_cldids);
	foreach($dm_cldids as $dm_cldid) {			
		if(!isset($dm_reached_brkdown[$dm_uid][$dm_cldid]))	unset($dm_notes_c[$dm_uid][$dm_cldid]);
	}
}*/
foreach ($dm_calls['positive'] as $usr_id => $pos_dm) {
	$this_users_dms = $dm_reached_brkdown[$usr_id];
	if (empty($this_users_dms)) unset($dm_calls['positive'][$usr_id]);
	foreach ($pos_dm as $pos_cldid => $n) {
		if (!in_array($pos_cldid, array_keys($this_users_dms))) {
			$users_unset[$usr_id]++;
			unset($dm_calls['positive'][$usr_id][$pos_cldid]);
		} else {
			$dm_calls['positive'][$usr_id][$pos_cldid] = $dm_reached_brkdown[$usr_id][$pos_cldid];   //to prevent pos calls from going higher than dm_reached
		}
	}
}
#end   dm_reached##


#remove those with less than 5 calls
foreach ($clients as $hrchy_id => $client_d) {
	foreach ($client_d as $client_id => $client) {

		if ($client['count'] < 5 && !($succ_calls['client'][$clients_ids[$client_id]])) {

			unset($clients_ids[$client_id]);
			unset($clients[$hrchy_id][$client_id]);
		}
	}
}

foreach ($agent_array as $user_id => $val) {
	$agent_array[$user_id]['dm_reached'] = 0;
	foreach ($engagesment_breakdown['agent'][$user_id]['engagements'] as $client_list_id => $value) {
		foreach ($value as $ch => $val) {
			foreach ($val as $v) {
				$agent_array[$user_id]['dm_reached'] += $v['count'];
			}
		}
	}
}

//dataprofilo - requery names of clients
if ($logged_user == 1236545) {

	foreach ($clients as $hierarch => $cs) {
		if (!empty($cs)) :
			$sql = "SELECT company, clients.client_id FROM `clients` 
				INNER JOIN `target_details` td USING (`target_detail_id`) 
				INNER JOIN `client_accounts` ca USING (`client_id`)
				/*INNER JOIN `hierarchy_tree` h USING (`hierarchy_tree_`)*/
				INNER JOIN `comp_details` USING (`comp_detail_id`) 
				INNER JOIN `companies` USING (`company_id`)
				WHERE clients.client_id IN (" . implode(",", array_keys($cs)) . ")
				GROUP BY client_id ";
			$db->query($sql);
			while ($row = $db->fetch_assoc()) {
				$clients_ids[$row['client_id']] = $row['company'];
			}
		else :
			unset($clients[$hierarch]);
		endif;
	}
}

/* if ($_SERVER['REMOTE_ADDR'] == '192.168.60.172') {
	echo '<pre>' . print_r($clients, true) . '</pre>';
} */

################jay-ar###########################
$total_emails = 0;
function retrieveEmailsAcquired($val)
{ //this should be client_id
	global  $db;
	global $total_emails;
	global $dfrom;
	global $dto;
	global $from;
	global $to;
	global $apac;

	if ($apac) $tz = "Asia/Manila";
	else $tz = "US/Pacific";

	$filter = " AND CONCAT_WS(' ', `edit_date`, `edit_time`) BETWEEN CONVERT_TZ('{$from}', '{$tz}', 'UTC' ) AND CONVERT_TZ('{$to}','{$tz}', 'UTC' )  ";
	//$filter = " AND `edit_date` BETWEEN date(CONVERT_TZ('$dfrom', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$dto','{$tz}', 'UTC' ))";

	$sql = "SELECT count(*)
			AS email_count
			FROM `clients`
			INNER JOIN client_accounts USING (client_id)
			INNER JOIN client_job_orders USING (client_account_id)
			INNER JOIN client_lists USING (client_job_order_id)
			INNER JOIN tm_edits_log USING (client_list_id)
			WHERE `client_id` = '{$val}'
			AND tm_edits_log.`to_value` != ''
			";

	$sql .=	$filter;
	$date2 = new DateTime($dto);
	$date1 = new DateTime("2015-06-07");
	$diff = $date2->diff($date1)->format("%a");

	if ($diff > -1) :
		$sql .=	"AND edit_type IN ('fresh_email')";
	else :
		$sql .=	"AND edit_type IN ('email') AND from_value = ''";
	endif;


	$db->query($sql);
	$rows = $db->fetch_assoc();
	if (!$rows) :
		return $db->error();
	else :
		$total_emails += $rows['email_count'];
		return $rows['email_count'];
	endif;
}
function retrieveEmailsAcquiredValid($val)
{ //this should be client_id
	global $db;
	global $total_emails_valid;
	global $dfrom;
	global $dto;
	global $from;
	global $to;
	global $apac;

	if ($apac) $tz = "Asia/Manila";
	else $tz = "US/Pacific";

	$filter = " AND CONCAT_WS(' ', `edit_date`, `edit_time`) BETWEEN CONVERT_TZ('{$from}', '{$tz}', 'UTC' ) AND CONVERT_TZ('{$to}','{$tz}', 'UTC' )  ";
	//$filter = " AND `edit_date` BETWEEN date(CONVERT_TZ('$dfrom', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$dto','{$tz}', 'UTC' ))";

	$sql = "SELECT count(*)
			AS email_count
			FROM `clients`
			INNER JOIN client_accounts USING (client_id)
			INNER JOIN client_job_orders USING (client_account_id)
			INNER JOIN client_lists USING (client_job_order_id)
			INNER JOIN tm_edits_log USING (client_list_id)
			INNER JOIN  `validated_emails` ON ( tm_edits_log.to_value = validated_emails.email ) 
			WHERE `client_id` = '{$val}'
			AND tm_edits_log.`to_value` != ''
			";

	$sql .=	$filter;
	$date2 = new DateTime($dto);
	$date1 = new DateTime("2015-06-07");
	$diff = $date2->diff($date1)->format("%a");

	if ($diff > -1) :
		$sql .=	"AND edit_type IN ('fresh_email')";
	else :
		$sql .=	"AND edit_type IN ('email') AND from_value = ''";
	endif;


	$db->query($sql);
	$rows = $db->fetch_assoc();
	if (!$rows) :
		return $db->error();
	else :
		$total_emails_valid += $rows['email_count'];
		return $rows['email_count'];
	endif;
}





function countEmailsByUser($user_id)
{
	global $db;
	global $total_emails_by_user;
	global $dfrom;
	global $dto;
	global $from;
	global $to;
	global $apac;

	if ($apac) $tz = "Asia/Manila";
	else $tz = "US/Pacific";

	$filter = " AND CONCAT_WS(' ', `edit_date`, `edit_time`) BETWEEN CONVERT_TZ('{$from}', '{$tz}', 'UTC' ) AND CONVERT_TZ('{$to}','{$tz}', 'UTC' )  ";
	//$filter = " AND `edit_date` BETWEEN date(CONVERT_TZ('$dfrom', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$dto','{$tz}', 'UTC' ))";

	$verified_d = array();

	//	$sql  = "SELECT COUNT(*) as emails FROM tm_edits_log INNER JOIN clients USING(target_detail_id) WHERE client_id = '{$val}' AND edit_type = 'email' AND from_value = '' ";
	$sql = "SELECT target_detail_id
			
			FROM `tm_edits_log`
			WHERE `user_id` = '{$user_id}' ";


	$sql .=	$filter;
	$date2 = new DateTime($dto);
	$date1 = new DateTime("2015-06-07");
	$diff = $date2->diff($date1)->format("%a");

	if ($diff > -1) :
		$sql .=	" AND edit_type IN ('fresh_email')";
	else :
		$sql .=	" AND edit_type IN ('email') AND from_value = ''";
	endif;
	$sql .=	" AND tm_edits_log.`to_value` != '' ";

	$db->query($sql);
	while ($rows = $db->fetch_assoc()) {
		$verified_d[$rows['target_detail_id']] = $rows['target_detail_id'];
	}

	$total_emails_by_user += count($verified_d);
	return $verified_d;
}
function countEmailsByUserValid($user_id)
{
	global  $db;
	global $total_emails_by_user_valid;
	global $dfrom;
	global $dto;
	global $from;
	global $to;
	global $apac;
	if ($apac) $tz = "Asia/Manila";
	else $tz = "US/Pacific";

	$filter = " AND CONCAT_WS(' ', `edit_date`, `edit_time`) BETWEEN CONVERT_TZ('{$from}', '{$tz}', 'UTC' ) AND CONVERT_TZ('{$to}','{$tz}', 'UTC' )  ";
	//$filter = " AND `edit_date` BETWEEN date(CONVERT_TZ('$dfrom', '{$tz}', 'UTC' )) AND date(CONVERT_TZ('$dto','{$tz}', 'UTC' ))";


	//	$sql  = "SELECT COUNT(*) as emails FROM tm_edits_log INNER JOIN clients USING(target_detail_id) WHERE client_id = '{$val}' AND edit_type = 'email' AND from_value = '' ";
	$sql = "SELECT count(*)
			AS email_count
			FROM `tm_edits_log`
			INNER JOIN  `validated_emails` ON ( tm_edits_log.to_value = validated_emails.email ) 
			WHERE `user_id` = '{$user_id}' ";
	$sql .=	$filter;
	$date2 = new DateTime($dto);
	$date1 = new DateTime("2015-06-07");
	$diff = $date2->diff($date1)->format("%a");

	if ($diff > -1) :
		$sql .=	" AND edit_type IN ('fresh_email')";
	else :
		$sql .=	" AND edit_type IN ('email') AND from_value = ''";
	endif;
	$sql .=	" AND tm_edits_log.`to_value` != '' ";

	$db->query($sql);
	$rows = $db->fetch_assoc();
	if (!$rows) :
		return $db->error();
	else :
		$total_emails_by_user_valid += $rows['email_count'];
		return $rows['email_count'];
	endif;
}

function countDeliveredEmails($type, $id)
{
	global $dot45;
	global $apac;
	global $dfrom;
	global $dto;
	global $from;
	global $to;
	global $total_delivered, $total_delivered_user;
	if ($apac) $tz = "Asia/Manila";
	else $tz = "US/Pacific";
	$delivered_info;
	$clause = "";
	$returnArray = array();

	# client or user #
	if ($type == "user") :
		$clause = " tm_edits_log.`user_id` = " . $id;
	elseif ($type == "client") :
		$clause = " `client_id` = " . $id;
	endif;

	# date filter #
	$filter = " AND CONCAT_WS(' ', `edit_date`, `edit_time`) BETWEEN CONVERT_TZ('{$from}', '{$tz}', 'UTC' ) AND CONVERT_TZ('{$to}','{$tz}', 'UTC')  
	/*AND date_received BETWEEN CONVERT_TZ('{$dfrom}', '{$tz}', 'UTC' ) AND CONVERT_TZ('{$dto}','{$tz}', 'UTC')*/ ";

	# query #
	$sql = "SELECT 
				mailer_events.mailer_event_id,to_value,client_id ,receipt_type, bounce_type
			FROM `clients`
				INNER JOIN client_accounts USING (client_id)
				INNER JOIN client_job_orders USING (client_account_id)
				INNER JOIN client_lists USING (client_job_order_id)
				INNER JOIN tm_edits_log USING (client_list_id)
				INNER JOIN mailer_events ON (mailer_events.to_email = tm_edits_log.to_value)
				INNER JOIN mailer_event_receipts ON (mailer_event_receipts.mailer_event_id = mailer_events.mailer_event_id)
			WHERE {$clause}
			AND edit_type = 'fresh_email' {$filter} ";
	$fresh_email_res = $dot45->query($sql);

	# reset counters #
	$count = 0;
	$bounce = 0;
	$hard = 0;
	$soft = 0;
	$uncategorized = 0;

	# counters counting (lol) #
	while ($fresh_email_row = $fresh_email_res->fetch_assoc()) :
		$count++;
		if ($fresh_email_row['receipt_type'] == "bounce") :
			$bounce++;
			if ($fresh_email_row['bounce_type'] == "hard") :
				$hard++;
			elseif ($fresh_email_row['bounce_type'] == "soft") :
				$soft++;
			else :
				$uncategorized++;
			endif;
		endif;
	endwhile;

	# building the return array #
	$delivered = $count - $bounce;
	$returnArray['count'] = $count;
	$returnArray['bounces'] = $bounce;
	$returnArray['hard'] = $hard;
	$returnArray['soft'] = $soft;
	$returnArray['uncategorized'] = $uncategorized;
	$returnArray['delivered'] = $delivered;

	# for total count (global variable) #
	if ($type == "user") :
		$total_delivered_user += $delivered;
	elseif ($type == "client") :
		$total_delivered += $delivered;
	endif;

	return $returnArray;
}

#####################################################

function unit($val, $unit)
{
	if (empty($val)) return "";
	if ($val > 1) return "<b>{$val}</b> <i>{$unit}s</i>";
	return "<b>{$val}</b> <i>{$unit}</i>";
}

function print_time($num_secs)
{
	$h = floor($num_secs / 3600);
	$m = floor(($num_secs - ($h * 3600)) / 60);

	echo unit($h, "hr") . " " . unit($m, "min");
}
function get_data($user_id, $tz, $tstart, $tend, $acct_id)
{
	global  $db;
	global  $now;
	global  $nowx;
	global $acct_hid;
	//global $user_filter;
	global $my_team;
	global  $apac;
	global $test;

	if (!empty($my_team)) $user_filter = implode(",", $my_team);
	if ($user_id != 0) $user = "user_id = {$user_id} AND";
	else if ($user_id == 0) $user = "";
	if ($user_filter) $user .= " user_id IN ($user_filter) AND ";

	if ($acct_id) $filter = " AND client_account_id IN ({$acct_id}) ";
	else $filter = "";

	$tz2 = "Asia/Manila";

	if ($apac == 1) {
		//if ($acct_hid[$acct_id] == 21 || $acct_hid[$acct_id] == 123 || $acct_hid[$acct_id] == 136){
		$tz = "Asia/Manila";
		$end = $nowx->dt('Y-m-d');
	} else {
		$tz = "US/Pacific";
		$end = $now->dt('Y-m-d');
	}

	if (date("Y-m-d", strtotime($tend)) >= $end)
		$sql = "SELECT `client_account_id`, user_id, last_name, first_name,adjustment, CONVERT_TZ(`login`, 'UTC', '{$tz2}') as tzin, CONVERT_TZ(`logout`, 'UTC', '{$tz2}') as tzout 
		FROM `dtr_account_logs` INNER JOIN employees USING (user_id)  WHERE {$user} `login` >= CONVERT_TZ('$tstart', '{$tz}', 'UTC') AND `logout` <= CONVERT_TZ('$tend', '{$tz}','UTC') {$filter} AND dtr_account_logs.`x` = 'active'";
	else
		$sql = "SELECT `client_account_id`, user_id, last_name, first_name,adjustment, CONVERT_TZ(`login`, 'UTC', '{$tz2}') as tzin, CONVERT_TZ(`logout`, 'UTC', '{$tz2}') as tzout 
		FROM `dtr_account_logs` INNER JOIN employees USING (user_id) WHERE {$user}  `login` >= CONVERT_TZ('$tstart', '{$tz}','UTC') AND `logout` <= CONVERT_TZ('$tend',  '{$tz}','UTC') {$filter} AND `logout` != '0000-00-00' AND dtr_account_logs.`x` = 'active'";
	//AND `login` >= CONVERT_TZ('$tstart', 'UTC', '{$tz}') AND `logout` <= CONVERT_TZ('$tend', 'UTC', '{$tz}') 
	//AND `login` >= '$tstart' AND `logout` <= '$tend' 
	if ($_REQUEST['test'] == 2) {
		echo $sql;
	}

	$db->query($sql); // AND `logout` != '0000-00-00'
	$arow = array();
	//echo $sql;
	while ($row = $db->fetch_assoc()) {
		if ($row['tzout'] == NULL || $row['tzout'] == '0000-00-00 00:00:00') $row['tzout'] = $nowx->dt('Y-m-d H:i:s');
		//echo $row['tzin']. ' -' .$row['tzout']. '<br />';
		$arow[] = $row;
	}

	return $arow;
}
function get_hids($user_id)
{
	global  $db;
	$sql = "SELECT hierarchy_tree_id FROM `hierarchy_tree_details` WHERE `user_id` = {$user_id} AND x ='active'";
	$db->query($sql);

	while ($row = $db->fetch_assoc()) :
		$downline_h3_ids[] = $row['hierarchy_tree_id'];
	endwhile;
	return $downline_h3_ids;
}

function fetch_emails_sent($user_id, $tstart, $tend, $client_id = 0)
{

	/*	for mailer events  queries if you change the sql here you should also do the same on these files: emails.php and email-reports.php */
	global $apac;
	global  $db;
	global $node;

	$results = array();
	$bus_id = $node['hierarchy_tree_id'];
	if (in_array($bus_id, array(7, 8))) $filter = "";
	else $filter = "and hierarchy_tree_id = {$bus_id}";

	$prim_filter = "";

	if (empty($user_id) && empty($client_id))	return  $data['count'] = 0;

	if (!empty($user_id)) $prim_filter .= " AND user_id = {$user_id} ";
	if (!empty($client_id)) $prim_filter .= " AND client_id = {$client_id} ";

	if ($apac) $tz = "Asia/Manila";
	else $tz = "US/Pacific";

	if ($tstart != 0 && $tend != 0) {
		$sent_f = " `sent` BETWEEN CONVERT_TZ('{$tstart}', '{$tz}', 'UTC') 
				AND CONVERT_TZ('{$tend}', '{$tz}', 'UTC')";
	} else {
		$sent_f = " `sent` = '0000-00-00 00:00:00' and mailer_event_id > 193325 ";
	}


	/*	for mailer events  queries if you change the sql here you should also do the same on these files: emails.php and email-reports.php */

	$sql = "SELECT * FROM `mailer_events` 
	  	INNER JOIN client_lists USING (`client_list_id`)	
		INNER JOIN client_job_orders USING (`client_job_order_id`)				
		INNER JOIN `client_accounts` USING (  `client_account_id` ) 
		WHERE 
		{$sent_f}
		AND source IN ('save_n_send') and client_accounts.x='active'
		{$prim_filter}
		/*$filter*/ ";

	$db->query($sql);
	while ($row =  $db->fetch_assoc()) {
		$results[$row['source']][] = $row['to_email'];
		$results_agent['sent'][$row['source']][$row['user_id']][] = $row['to_email'];
	}


	$data['count'] = count($results['save_n_send']);
	//$data['emails']  = $results['save_n_send'];
	$data['count_mailer_triggers'] = count($results['mailer_trigger']);

	/*	for mailer events  queries if you change the sql here you should also do the same on these files: emails.php and email-reports.php */
	if ($tstart != 0 && $tend != 0) {
		/*query for opened emails -- added nov2012 */

		$sql = "SELECT * FROM `mailer_events` 
			 INNER JOIN client_lists USING (`client_list_id`)	
			 INNER JOIN client_job_orders USING (`client_job_order_id`)				
			 INNER JOIN `client_accounts` USING (  `client_account_id` ) 
			 INNER JOIN `mailer_event_receipts` mer  USING (`mailer_event_id`) 
			 WHERE 
			 {$sent_f}
			 AND receipt_type = 'open' AND source IN ('save_n_send') and client_accounts.x='active'
			 {$prim_filter}
			/*$filter*/";

		$db->query($sql);
		while ($row =  $db->fetch_assoc()) {
			$opened[$row['source']][] = $row['to_email'];
			$results_agent['opened'][$row['source']][$row['user_id']][] = $row['to_email'];
		}

		//echo $sql . "<br />";
		$data['opened_count'] = count($opened['save_n_send']);
		$data['opened_emails']  = $opened['save_n_send'];
		$data['opened_count_mailer_triggers'] = count($opened['mailer_trigger']);

		/* end query for opened emails */

		/*	for mailer events  queries if you change the sql here you should also do the same on these files: emails.php and email-reports.php */
		/*query for bounced emails -- added jun2013 */

		$sql = "SELECT * FROM `mailer_events` 
			 INNER JOIN client_lists USING (`client_list_id`)	
			 INNER JOIN client_job_orders USING (`client_job_order_id`)				
			 INNER JOIN `client_accounts` USING (  `client_account_id` ) 
			 INNER JOIN `mailer_event_receipts` mer  USING (`mailer_event_id`) 
			 WHERE 
			 {$sent_f}
			 AND receipt_type = 'bounce' and bounce_type='hard' AND source IN ('save_n_send') and client_accounts.x='active'
			 {$prim_filter}
			/*$filter*/";

		$db->query($sql);
		while ($row =  $db->fetch_assoc()) {
			$hardb[$row['source']][] = $row['to_email'];
			$results_agent['hardb'][$row['source']][$row['user_id']][] = $row['to_email'];
		}

		//echo $sql . "<br />";
		$data['hardb_count'] = count($hardb['save_n_send']);
		$data['hardb_emails']  = $hardb['save_n_send'];
		$data['hardb_count_mailer_triggers'] = count($hardb['mailer_trigger']);

		/* end query for opened emails */
	}
	$data['per_agent'] = $results_agent;

	return $data;
}


$team_manhours = array();
foreach ($my_team as $uid) {
	$data = get_data($uid, " ", $from, $to, ""); # -> agent's logs
	$adjustments = 0;
	$result_total = 0;
	if (!empty($data))
		foreach ($data as $null => $contents) :
			$tzstart = strtotime($contents[tzin]);
			$tzend = strtotime($contents[tzout]);
			$result = $tzend - $tzstart;
			if ($result > 32400)  $result = 32400;             //don't include if log is > 8.5hrs
			else { // echo $contents['tzin']. ' -' .$contents['tzout']. '<br />';
				$dttt = date("M j, h:iA", strtotime($contents['tzin']));
				//echo "$dttt";
				$dttt = date("M j, h:i A", strtotime($contents['tzout']));
				//echo " - $dttt<br />";
			}
			$adjustments += ($contents[adjustment]);
			$result_total += $result;

		endforeach;
	if ($result_total && $adjustments) $result_total += $adjustments;

	if ($result_total) {
		$team_manhours[$uid] = $result_total;
		$agent_array[$uid]['name'] = htmlentities($contents['first_name'] . " " . $contents['last_name']);
	} else {
		$team_manhours[$uid] = 1; //temp 01-19-20
	}
}

function clean($str)
{
	$str = trim($str);
	$a = array(".", ",", "-", ":", " ");
	$replace = array("", "", "", "");
	foreach ($a as $m) $str = str_replace($a, "", $str);
	return $str;
}
//05-07-2019
function get_ext($user_id)
{

	global $dot45;
	$q = $dot45->query("SELECT extension as ext FROM employees WHERE user_id = $user_id");
	$row = $q->fetch_assoc();
	return $row['ext'];
}

function call_volume($from, $to, $ext)
{
	global $dot45;
	global $cdr_ilo;
	global $cdr_dvo;
	global $branch;
	global $tz;
	global $billsec;

	if (!$ext)
		return 0;

	$sql = "SELECT CONVERT_TZ('{$from}','{$tz}','America/Los_Angeles') as d_from, 
	 CONVERT_TZ('{$to}','{$tz}','America/Los_Angeles') as d_to";

	$q = $dot45->query($sql);

	$dates = $q->fetch_assoc();

	$sql = "SELECT COUNT(uniqueid) as num_calls
			FROM `cdr`
			WHERE (dcontext='outgoing' OR dcontext='incoming' 
			       OR dcontext LIKE 'pbx%' OR dcontext LIKE 'predialer-num' 
			       OR dcontext LIKE 'predialer-noamd'
			       OR dcontext='salesforce-outbound')
			AND (lastapp='Hangup' OR lastapp='Queue' OR lastapp='Dial')
			AND  (dst NOT IN(601,602,603,999))
			AND (LENGTH(dst)>=10 or dst IN ('s','h'))
			AND calldate BETWEEN '{$dates["d_from"]}'
			AND '{$dates["d_to"]}'
			AND billsec  >= 0
			AND agent_id = \"Agent/{$ext}\"";

	if ($branch == "iloilo")
		$q = $cdr_ilo->query($sql);
	else
		$q = $cdr_dvo->query($sql);

	$res = $q->fetch_assoc();

	return $res["num_calls"] ?: 0;
}
