package dao.Impl;

import com.eastday.domain.AppFlowDomain;

import dao.AppFlowInsertDao;
import jdbc.JDBCHelper;

/**
 * Created by admin on 2018/6/22.
 */
public class AppFlowInsertDaoImpl  implements AppFlowInsertDao{
    private JDBCHelper jdbcHelper = JDBCHelper.getInstance();
    @Override
    public int del(String dt, String hour) {
        String sql ="delete from rpt_minwap_app_flow_show_active_result where dt=? and hour =? ";
        Object[] param =new Object[]{dt,hour};

        return jdbcHelper.executeUpdate(sql,param);
    }

    @Override
    public int insert(AppFlowDomain domain) {
        String sql ="insert into rpt_minwap_app_flow_show_active_result(dt,hour,urltype,url,title,rate) " +
                "values (?,?,?,?,?,?)";
        Object[] params = new Object[]{domain.dt(),domain.hour(),domain.type1(),
                domain.url(),domain.title(),domain.rate()};

        return jdbcHelper.executeUpdate(sql,params);
    }
    public static void main(String[] args) {
        AppFlowInsertDaoImpl domain =new AppFlowInsertDaoImpl();
        domain.del("2018010101",null);
    }
}
