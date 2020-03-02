package spark.getMac;


import com.alibaba.fastjson.JSON;

import java.io.Serializable;

public abstract class BaseMacInfo implements Serializable {
    /**
     * 数据唯一键
     */
    private String rowkey;
    /**
     * 采集时间
     */
    private long collectTime;
    /**
     * 联动设备id
     */
    private String linkageDevCode;
    /**
     * 设备编码，海康设备以平台code表示
     */
    private String devNo;
    /**
     * 场所编号
     */
    private String siteNo;
    /**
     * 采集设备经度
     */
    private double collectDevLongitude;
    /**
     * 采集设备纬度
     */
    private double collectDevDimensionality;
    /**
     * 设备类型
     */
    private int devType;
    /**
     * 消息类型
     */
    private int msgType;
    /**
     * 设备厂商
     */
    private String vendor;
    /**
     * 协议版本
     */
    private String version;

    /**
     * 采集时间记录集，以逗号分隔
     */
    private String collectTimeRecord;

    private String linkFaceMacId;

    private  String linkVehicleMacId;

    private String linkBodyMacId;

    private String linkRFIDMacId;

    /**
     * 终端品牌
     */
    private String areaCode;

    public void setLinkFaceMacId(String linkFaceMacId) {
        this.linkFaceMacId = linkFaceMacId;
    }

    public void setLinkVehicleMacId(String linkVehicleMacId) {
        this.linkVehicleMacId = linkVehicleMacId;
    }

    public void setLinkBodyMacId(String linkBodyMacId) {
        this.linkBodyMacId = linkBodyMacId;
    }

    public void setLinkRFIDMacId(String linkRFIDMacId) {
        this.linkRFIDMacId = linkRFIDMacId;
    }

    public String getLinkFaceMacId() {

        return linkFaceMacId;
    }

    public String getLinkVehicleMacId() {
        return linkVehicleMacId;
    }

    public String getLinkBodyMacId() {
        return linkBodyMacId;
    }

    public String getLinkRFIDMacId() {
        return linkRFIDMacId;
    }

    public long getCollectTime() {
        return collectTime;
    }

    public void setCollectTime(long collectTime) {
        this.collectTime = collectTime;
    }

    public int getMsgType() {
        return msgType;
    }

    public void setMsgType(int msgType) {
        this.msgType = msgType;
    }

    public String getLinkageDevCode() {
        return linkageDevCode;
    }

    public void setLinkageDevCode(String linkageDevCode) {
        this.linkageDevCode = linkageDevCode;
    }

    public String getDevNo() {
        return devNo;
    }

    public void setDevNo(String devNo) {
        this.devNo = devNo;
    }

    public String getSiteNo() {
        return siteNo;
    }

    public void setSiteNo(String siteNo) {
        this.siteNo = siteNo;
    }

    public double getCollectDevLongitude() {
        return collectDevLongitude;
    }

    public void setCollectDevLongitude(double collectDevLongitude) {
        this.collectDevLongitude = collectDevLongitude;
    }

    public double getCollectDevDimensionality() {
        return collectDevDimensionality;
    }

    public void setCollectDevDimensionality(double collectDevDimensionality) {
        this.collectDevDimensionality = collectDevDimensionality;
    }

    public int getDevType() {
        return devType;
    }

    public void setDevType(int devType) {
        this.devType = devType;
    }

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCollectTimeRecord() {
        return collectTimeRecord;
    }

    public void setCollectTimeRecord(String collectTimeRecord) {
        this.collectTimeRecord = collectTimeRecord;
    }

    public String getAreaCode() {
        return areaCode;
    }

    public void setAreaCode(String areaCode) {
        this.areaCode = areaCode;
    }
    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}

