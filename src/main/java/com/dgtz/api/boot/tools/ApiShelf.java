package com.dgtz.api.boot.tools;

import com.brocast.riak.api.beans.DcMediaEntity;
import com.brocast.riak.api.dao.RiakAPI;
import com.brocast.riak.api.dao.RiakTP;
import com.brocast.riak.api.factory.IRiakQueryFactory;
import com.brocast.riak.api.factory.IRiakSaveFactory;
import com.brocast.riak.api.factory.RiakQueryFactory;
import com.brocast.riak.api.factory.RiakSaveFactory;
import com.dgtz.db.api.dao.db.DBUtils;
import com.dgtz.db.api.dao.db.JDBCUtil;
import com.dgtz.mcache.api.factory.Constants;
import com.dgtz.mcache.api.factory.RMemoryAPI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
public class ApiShelf {


    public String getStreamerIP(Long idMedia) {
        String ip = null;
        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;

        try {
            dbConnection = JDBCUtil.getDBConnection();
            preparedStatement = dbConnection.prepareStatement(
                    "SELECT ip FROM dc_user_stats WHERE id_media = ? AND action_type in (20, 21);");

            preparedStatement.setLong(1, idMedia);
            rs = preparedStatement.executeQuery();
            while (rs.next()) {
                ip = rs.getString("ip");
            }

        } catch (Exception e) {
            e.printStackTrace();
            DBUtils.dbRollback(dbConnection);
        } finally {
            DBUtils.closeConnections(preparedStatement, dbConnection, rs);
        }

        return ip;
    }

    public void updateContentRating(long rating, long idMedia){
        RiakTP transport = RiakAPI.getInstance();
        IRiakQueryFactory queryFactory = new RiakQueryFactory(transport);
        DcMediaEntity entity = queryFactory.queryMediaDataByID(idMedia);

        IRiakSaveFactory saveFactory = new RiakSaveFactory(transport);
        entity.rating = rating;
        saveFactory.updMediaContent(entity);
        RMemoryAPI.getInstance()
                .pushHashToMemory(Constants.MEDIA_KEY + entity.idMedia, "rating", rating+"");

    }
}
