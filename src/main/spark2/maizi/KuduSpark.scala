package com.maizi

import org.apache.spark.sql.SparkSession


object KuduSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Kudu")
//      .master("local")
      .getOrCreate()

    // 连接到kudu
//    val kuduDF = spark.read.format("org.apache.kudu.spark.kudu")
//      .option("kudu.master", "BI-HD02:7051")
//      .option("kudu.table", "impala::rt.finance_account")
//      .load()


    val dd = spark.read.format("org.apache.kudu.spark.kudu").option("kudu.master", "BI-HD02:7051")

    val invt_stage_quit_form = dd.option("kudu.table", "impala::rt.invt_stage_quit_form").load()
    val vip_account = dd.option("kudu.table", "impala::rt.vip_account").load()
    val finance_plan = dd.option("kudu.table", "impala::rt.finance_plan").load()
    val finance_plan_scope = dd.option("kudu.table", "impala::rt.finance_plan_scope").load()


    val user_login_detail_log = dd.option("kudu.table", "impala::rt.user_login_detail_log").load()
    val mkt_invest_ranking_config = dd.option("kudu.table", "impala::rt.mkt_invest_ranking_config").load()
    val invt_custom_trans_info = dd.option("kudu.table", "impala::rt.invt_custom_trans_info").load()

    val vip_account_mark_ranking_02 = dd.option("kudu.table", "impala::rt.vip_account_mark_ranking_02").load()


    //    kuduDF.show(3)

    // register a temporary table and use SQL
    invt_stage_quit_form.createOrReplaceTempView("invt_stage_quit_form")
    vip_account.createOrReplaceTempView("vip_account")
    finance_plan.createOrReplaceTempView("finance_plan")
    finance_plan_scope.createOrReplaceTempView("finance_plan_scope")

    user_login_detail_log.createOrReplaceTempView("user_login_detail_log")


    mkt_invest_ranking_config.createOrReplaceTempView("mkt_invest_ranking_config")
    invt_custom_trans_info.createOrReplaceTempView("invt_custom_trans_info")

    vip_account_mark_ranking_02.createOrReplaceTempView("vip_account_mark_ranking_02")



    //    val filteredDF = spark.sql("select count(1) from kudu_table").show()


    //    val sql1 ="SELECT to_date(a.service_time) as day\n,hour(a.service_time) as hour\n,decode(a.platform,1,'nono',2,'csyy','NVL') as biz\n,nvl(cast (b.channel_id as STRING),'NVL') as channel_id\n,nvl(e.am_name,'NVL') as  channel_name\n,nvl(b.channel_detail,'NVL') as channel_detail\n,count(1) as \"20009\"\n,count(distinct a.user_id) as '20003'\n,sum(a.amount) as '20001'\nfrom rt.vip_form a\nINNER JOIN rt.user_info c \nON a.user_id=c.id and is_special=0\nand c.user_type IN (0,1)\nand c.id NOT IN (920272,1598,1600,1176674)\nand to_date(a.service_time)='2019-07029'\nand a.service_status = 1\nleft join rt.user_approach_info b on a.user_id=b.user_id\nleft join rt.admin e\non e.id=b.channel_id\nleft join rt.system_config  d\non c.user_name=d.config_value\nAND d.config_name LIKE '%dfd%'\nwhere  d.id is null\ngroup by 1,2,3,4,5,6"


    //    val sql1 = "select  to_date(current_timestamp()) as dt,hour(current_timestamp()) as ht,\n(case when fp.platform_type=1 then 'nono' when fp.platform_type=2 then 'csyy' end) as biz\n,case  when fp.scope = 29  then  fp.title\nelse fsp.label end as product\n,sum(va.amount - coalesce(qf.quit_amount, 0)) as sd\n,count(distinct va.user_id) as `20014`\nFROM rt.vip_account va \njoin rt.finance_plan fp\n    on va.fp_id = fp.id and va.is_cash=0\nleft join (select va_id, sum(quit_amount) quit_amount\n            from rt.invt_stage_quit_form\n            where quit_mode <> 2 and scope = 26 and quit_status = 2 \n             group by va_id) qf\non va.id = qf.va_id\nleft join rt.finance_plan_scope fsp\non fsp.id = fp.scope\ngroup by dt,ht,biz,product"

    //    val sql1 = "select to_date(now()) as day\n,hour(now()) as hour\n, sum(fa.balance) as  mm --总余额\nfrom finance_account fa\nwhere fa.user_id NOT IN(166,168,169,170,920272,1598,1600,13936,1060684\n,1404313,1245897,29462110) --去除特殊账户\nand fa.role_id = 13"

//    val sql1 = "SELECT \nto_date(isqf.quit_success_time) as `day`\n,hour(isqf.quit_success_time) as `hour`\n,(case when a.platform=1 then 'nono' when a.platform=2 then 'csyy' end) as biz\n,case  when b.scope = 29  then  b.title else fps.label end as product\n,sum(isqf.quit_amount) as `20001`\nfrom invt_stage_quit_form isqf \ninner join vip_account a \n  on isqf.va_id=a.id and isqf.quit_status = 2 and isqf.quit_success_time>='${T}'\nleft join finance_plan b \n  on a.fp_id=b.id\nleft join finance_plan_scope fps\n  on b.scope=fps.id\ngroup by to_date(isqf.quit_success_time)\n,hour(isqf.quit_success_time)\n,case when a.platform=1 then 'nono' when a.platform=2 then 'csyy' end\n,case  when b.scope = 29  then  b.title else fps.label end"

//    val sql1 = "select `day`,\n`hour`,\ncount(1) `40009`\nfrom \n(\nselect to_date(ld.logon_time) `day`,\nHOUR(ld.logon_time) AS `hour`\nfrom user_login_detail_log ld\nwhere \nld.logon_time >= date_add(current_date(),-1)\nand ld.login_biz ='mzjk'\n) a\ngroup by `day`,\n`hour`"


//    val sql1 = "select t.*,row_number() over(order by annual_money desc,max_time asc) as rn\nfrom (select cf.id,\nva.user_id,\nmax(va.create_time) as max_time,\nsum(va.amount) as amount,\nsum(\n    case when fp.scope = 28 then round(va.amount,2) * csi.locking_expect / if(csi.locking_expect_unit = 0, 12, 365) --贵宾定制计算，期限是可以定制化的\n    else round(va.amount,2) * fp.expect / if(fp.expect_unit = 0, 12, 365)\n    end) annual_money --年化金额\nfrom finance_plan fp \njoin  /* +BROADCAST */ mkt_invest_ranking_config cf\non find_in_set(cast(fp.scope as string),cf.stat_product_scope)>=1\nand cf.stat_start_time <='2019-05-14'\nand cf.stat_end_time>='2019-05-14'\njoin vip_account va \non va.fp_id=fp.id\nand va.platform = 1\nand va.is_cash=0\nand to_date(va.create_time)='2019-05-14'\nleft join /* +BROADCAST */ invt_custom_trans_info csi \non csi.trans_id = va.trans_id\ngroup by cf.id,va.user_id) t"

//    val sql1 = "select t.*,row_number() over(order by annual_money desc,max_time asc) as rn\nfrom (select cf.id,\nva.user_id,\nmax(va.create_time) as max_time,\nsum(va.amount) as amount,\nsum(\n    case when fp.scope = 28 then round(va.amount,2) * csi.locking_expect / if(csi.locking_expect_unit = 0, 12, 365)\n    else round(va.amount,2) * fp.expect / if(fp.expect_unit = 0, 12, 365)\n    end) annual_money\nfrom finance_plan fp \njoin mkt_invest_ranking_config cf\non cf.stat_start_time <='2019-05-14'\nand cf.stat_end_time>='2019-05-14'\njoin vip_account_mark_ranking_02 va \non va.fp_id=fp.id\nand va.platform = 1\nand va.is_cash=0\nand to_date(va.create_time)='2019-05-14'\nleft join invt_custom_trans_info csi \non csi.trans_id = va.trans_id\nwhere find_in_set(cast(fp.scope as string),cf.stat_product_scope)>=1\ngroup by cf.id,va.user_id) t"


    val sql1 = "select t.*,row_number() over(order by annual_money desc,max_time asc) as rn\nfrom (select cf.id,\nva.user_id,\nmax(va.create_time) as max_time,\nsum(va.amount) as amount,\nsum(\n    case when fp.scope = 28 then round(va.amount,2) * csi.locking_expect / if(csi.locking_expect_unit = 0, 12, 365) --贵宾定制计算，期限是可以定制化的\n    else round(va.amount,2) * fp.expect / if(fp.expect_unit = 0, 12, 365)\n    end) annual_money --年化金额\nfrom finance_plan fp \njoin  /* +BROADCAST */ mkt_invest_ranking_config cf\non find_in_set(cast(fp.scope as string),cf.stat_product_scope)>=1\nand cf.stat_start_time <='2019-05-14'\nand cf.stat_end_time>='2019-05-14'\njoin vip_account va \non va.fp_id=fp.id\nand va.platform = 1\nand va.is_cash=0\nand to_date(va.create_time)='2019-05-14'\nleft join /* +BROADCAST */ invt_custom_trans_info csi \non csi.trans_id = va.trans_id\ngroup by cf.id,va.user_id) t"


    val filteredDF = spark.sql(sql1).show()

  }

}
