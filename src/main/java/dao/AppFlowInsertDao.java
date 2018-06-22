package dao;

import com.eastday.domain.AppFlowDomain;

/**
 * Created by admin on 2018/6/22.
 */
public interface AppFlowInsertDao {

    int del (String dt ,String hour);
    int insert (AppFlowDomain domain);
}
