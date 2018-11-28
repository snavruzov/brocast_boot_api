package com.dgtz.api.boot;

import com.brocast.riak.api.beans.DcMediaEntity;
import com.brocast.riak.api.beans.DcUsersEntity;
import com.brocast.riak.api.dao.RiakAPI;
import com.brocast.riak.api.dao.RiakTP;
import com.brocast.riak.api.factory.IRiakQueryFactory;
import com.brocast.riak.api.factory.RiakQueryFactory;
import com.dgtz.api.beans.MediaListInfo;
import com.dgtz.api.boot.beans.BasicResponse;
import com.dgtz.api.boot.constants.Errors;
import com.dgtz.api.boot.tools.ApiShelf;
import com.dgtz.api.contents.MediaShelf;
import com.dgtz.api.contents.UsersShelf;
import com.dgtz.api.enums.EnumErrors;
import com.dgtz.db.api.domain.MediaPublicInfo;
import com.dgtz.db.api.domain.PublicChannelsEntity;
import com.dgtz.db.api.domain.UserPublicInfo;
import com.dgtz.db.api.enums.EnumSQLErrors;
import com.dgtz.mcache.api.factory.Constants;
import com.dgtz.mcache.api.factory.RMemoryAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * BroCast.
 * Copyright: Sardor Navruzov
 * 2013-2016.
 */
@RestController
public class CoreController {
    private static final Logger log = LoggerFactory.getLogger(CoreController.class);

    @RequestMapping(value = "/video/del", method = RequestMethod.GET)
    public BasicResponse removeVideoContentById(@RequestParam("id") long idMedia,
                                                @RequestParam(value = "act", required=false, defaultValue="0") int action) {
        log.info("Video delete variable: {} {}", idMedia, action);
        BasicResponse basicResponse = null;
        try {

            if (idMedia > 0) {
                RiakTP transport = RiakAPI.getInstance();
                IRiakQueryFactory queryFactory = new RiakQueryFactory(transport);
                DcMediaEntity infoMem = queryFactory.queryMediaDataByID(idMedia);

                if (infoMem == null) {
                    basicResponse = new BasicResponse(Errors.NO_VIDEO_FOUND);
                } else {
                    EnumSQLErrors sqlErrors = new UsersShelf().removeTheMediaByOwner(infoMem.idUser, idMedia);
                    if (sqlErrors != EnumSQLErrors.OK) {
                        basicResponse = new BasicResponse(Errors.MEDIA_REMOVE_ERROR);
                    } else {
                        basicResponse = new BasicResponse();
                        switch (action){
                            case 1: {
                                String ip = new ApiShelf().getStreamerIP(idMedia);
                                if(ip!=null && !ip.isEmpty()) {
                                    RMemoryAPI.getInstance().pushSetElemToMemory("dc_users:blocked:ip", ip.trim());
                                }
                                break;
                            }
                            case 2: {
                                RMemoryAPI.getInstance().pushSetElemToMemory("dc_users:blocked:user", infoMem.getIdUser() + "");
                                break;
                            }
                        }
                    }
                }

            } else {
                basicResponse = new BasicResponse(Errors.WRONG_ID);
            }
        } catch (Exception e) {
            log.error("Error",e);
            basicResponse = new BasicResponse(Errors.SYSTEM_FAIL);
        }

        return basicResponse;
    }

    @RequestMapping(value = "/video/top", method = RequestMethod.GET)
    public BasicResponse topUpVideoContentById(@RequestParam("id") long idMedia,
                                                @RequestParam(value = "act", required=false, defaultValue="0") int action) {
        log.info("Video delete variable: {} {}", idMedia, action);
        BasicResponse basicResponse = null;
        try {

            if (idMedia > 0) {
                MediaListInfo infoMem = new MediaShelf().extractMediaByIdMedia(idMedia);
                if (infoMem == null) {
                    basicResponse = new BasicResponse(Errors.NO_VIDEO_FOUND);
                } else {
                    ApiShelf apiShelf = new ApiShelf();
                    basicResponse = new BasicResponse();
                    switch (action) {
                        case 1: {
                            apiShelf.updateContentRating(1, idMedia);
                            break;
                        }
                        case 2: {
                            apiShelf.updateContentRating(53, idMedia);
                            break;
                        }
                    }

                }

            } else {
                basicResponse = new BasicResponse(Errors.WRONG_ID);
            }
        } catch (Exception e) {
            log.error("Error",e);
            basicResponse = new BasicResponse(Errors.SYSTEM_FAIL);
        }

        return basicResponse;
    }

    @RequestMapping(value = "/video/{id}", method = RequestMethod.GET)
    public com.dgtz.api.beans.MediaListInfo listVideoContentById(@PathVariable("id") long idMedia) {
        log.info("Media variable: {}", idMedia);
        com.dgtz.api.beans.MediaListInfo contentResponse = null;
        try {
            if (idMedia > 0) {
                contentResponse = new MediaShelf().extractMediaByIdMedia(idMedia);
            }
        } catch (Exception e) {
            log.error("Error in video ID", e);
        }

        return contentResponse;
    }

    @RequestMapping(value = "/video/list", method = RequestMethod.GET)
    public List<MediaPublicInfo> listVideoContent(@RequestParam("off") int offset,
                                                  @RequestParam("lm") int limit) {
        List<MediaPublicInfo> contentResponse = new ArrayList<>();
        try {
                RiakTP transport = RiakAPI.getInstance();
                IRiakQueryFactory queryFactory = new RiakQueryFactory(transport);
                List<DcMediaEntity> mlist = queryFactory.queryMediaByLast(offset, limit);
                mlist.forEach(m -> {
                    MediaPublicInfo obj = new MediaPublicInfo();
                    String currentTime = String.valueOf(RMemoryAPI.getInstance().currentTimeMillis());
                    String username = RMemoryAPI.getInstance()
                            .pullHashFromMemory(Constants.USER_KEY + m.idUser, "username");
                    String verified = RMemoryAPI.getInstance()
                            .pullHashFromMemory(Constants.USER_KEY + m.idUser, "verified");
                    String thumb = Constants.encryptAmazonURL(m.idUser, m.idMedia, "jpg", "thumb", Constants.STATIC_URL);
                    String lcount = RMemoryAPI.getInstance()
                            .pullHashFromMemory(Constants.MEDIA_KEY + m.idMedia, "liked");
                    String vcount = RMemoryAPI.getInstance()
                            .pullHashFromMemory(Constants.MEDIA_KEY + m.idMedia, "vcount");
                    String avatar = RMemoryAPI.getInstance()
                            .pullHashFromMemory(Constants.USER_KEY + m.idUser, "avatar");
                    String evnt_time = RMemoryAPI.getInstance()
                            .pullHashFromMemory(Constants.MEDIA_KEY + m.idMedia, "evnt_time");

                    obj.setIdMedia(m.idMedia);
                    obj.setTitle(m.title);
                    obj.setDateadded(m.dateadded);
                    obj.setIdChannel(m.idChannel);
                    obj.setMethod(m.method);
                    obj.setStart_time(evnt_time);
                    obj.setIdUser(m.idUser);
                    obj.setUsername(username);
                    obj.setAmount(Long.valueOf(vcount==null?"0":vcount));
                    obj.setLiked(Long.valueOf(lcount==null?"0":lcount));
                    obj.setProgress(m.progress);
                    obj.setCurrenTime(currentTime);
                    obj.setRatio(m.ratio);
                    obj.setThumb(thumb);
                    obj.setVerified(verified == null ? false : Boolean.valueOf(verified));
                    obj.setAvatar(Constants.STATIC_URL + m.idUser + "/image" + avatar + "M.jpg");
                    obj.setDuration(m.duration.shortValue());
                    obj.setTags(m.tags);
                    obj.setLocation(m.location);
                    obj.setUrl(Constants.encryptAmazonURL(m.idUser, m.idMedia, "_hi.mp4", "v", Constants.VIDEO_URL));

                    contentResponse.add(obj);
                });


        } catch (Exception e) {
            log.error("Error in media list", e);
        }

        return contentResponse;
    }

    @RequestMapping(value = "/user/list", method = RequestMethod.GET)
    public List<UserPublicInfo> listUsersInfo(@RequestParam("off") int offset,
                                               @RequestParam("lm") int limit) {
        List<UserPublicInfo> contentResponse = new ArrayList<>();
        try {
            RiakTP transport = RiakAPI.getInstance();
            IRiakQueryFactory queryFactory = new RiakQueryFactory(transport);
            List<DcUsersEntity> mlist = queryFactory.queryUsersByLast(offset, limit);
            mlist.forEach(m -> {
                UserPublicInfo obj = new UserPublicInfo();

                obj.setIdUser(m.idUser);
                obj.setUsername(m.username);
                obj.setAbout(m.about);
                obj.setAvatar(Constants.STATIC_URL + m.idUser + "/image" + m.avatar + "M.jpg");
                obj.setCity(m.city);
                obj.setCountry(m.country);
                obj.setDate_reg(m.date_reg);
                obj.setEmail(m.email);
                obj.setVerified(m.verified);
                obj.setIdInbox(m.idInbox);
                obj.setIdFBSocial(m.idFBSocial);
                obj.setIdGSocial(m.idGSocial);
                obj.setIdTWTRSocial(m.idTWTRSocial);
                obj.setIdVKSocial(m.idVKSocial);
                obj.setHash(m.hash);
                obj.setSocial_links(m.social_links);

                contentResponse.add(obj);
            });


        } catch (Exception e) {
            log.error("Error in user list", e);
        }

        return contentResponse;
    }

    @RequestMapping(value = "/block/comment", method = RequestMethod.GET)
    public BasicResponse blockUserForComment(
                                        @RequestParam("idm") long idMedia,
                                        @RequestParam("idu") long idBlockUser) {

        BasicResponse basicResponse = null;
        try {
            log.info("Request variable: {}", idMedia, idBlockUser);
            UsersShelf shelf = new UsersShelf();
            DcUsersEntity userToBlock = shelf.getUserInfoById(idBlockUser);
            String idu = RMemoryAPI.getInstance().pullHashFromMemory(Constants.MEDIA_KEY + idMedia, "id_user");

            if (idu != null && Long.valueOf(idu) != idBlockUser && userToBlock != null && idBlockUser != 0) {
                EnumErrors errors = shelf.blockUserForComment(idMedia, Long.valueOf(idu), idBlockUser);
                if (errors == EnumErrors.NO_ERRORS) {
                    basicResponse = new BasicResponse();
                } else {
                    basicResponse = new BasicResponse(Errors.COMMENT_BLOCK_ERROR);
                }
            } else {
                basicResponse = new BasicResponse(Errors.WRONG_ID);
            }

        } catch (Exception e) {
            log.error("Error",e);
            basicResponse = new BasicResponse(Errors.SYSTEM_FAIL);
        }
        return basicResponse;
    }

    @RequestMapping(value = "/del/comment", method = RequestMethod.GET)
    public BasicResponse delUserForComment(
            @RequestParam("idm") long idMedia,
            @RequestParam("idc") long idComment) {

        BasicResponse basicResponse = null;
        try {
            log.info("Request variable: {}", idMedia, idComment);

            if (idMedia>0 && idComment > 0) {
                EnumErrors errors = new UsersShelf().removeUsersComment(idComment, 0, idMedia);
                if (errors == EnumErrors.NO_ERRORS) {
                    basicResponse = new BasicResponse();
                } else {
                    basicResponse = new BasicResponse(Errors.COMMENT_BLOCK_ERROR);
                }
            } else {
                basicResponse = new BasicResponse(Errors.WRONG_ID);
            }

        } catch (Exception e) {
            log.error("Error",e);
            basicResponse = new BasicResponse(Errors.SYSTEM_FAIL);
        }
        return basicResponse;
    }

    @RequestMapping(value = "/del/channel", method = RequestMethod.GET)
    public BasicResponse deleteChannel(
            @RequestParam("idch") long idChannel) {

        BasicResponse basicResponse = null;
        try {
            log.info("Request channel variable: {}", idChannel);

            if (idChannel>0) {
                UsersShelf usersShelf = new UsersShelf();
                PublicChannelsEntity channelInfo = new UsersShelf().extractChannelByIdChannel(idChannel, 0);
                if (channelInfo != null) {
                    EnumSQLErrors sqlErrors = EnumSQLErrors.OK;
                            //usersShelf.removeTheChannelByOwner(channelInfo.getOwnerIdUser(), idChannel, channelInfo.getAvatar());
                    if (sqlErrors == EnumSQLErrors.OK) {
                        basicResponse = new BasicResponse();
                    } else {
                        basicResponse = new BasicResponse(Errors.CHANNEL_REMOVE_ERROR);
                    }
                } else {
                    basicResponse = new BasicResponse(Errors.CHANNEL_NOT_FOUND);
                }
            } else {
                basicResponse = new BasicResponse(Errors.WRONG_ID);
            }

        } catch (Exception e) {
            log.error("Error",e);
            basicResponse = new BasicResponse(Errors.SYSTEM_FAIL);
        }
        return basicResponse;
    }

}
