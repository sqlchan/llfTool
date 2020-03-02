package spark.getMac;

import java.io.Serializable;

public final class TerminalInfo extends BaseMacInfo implements Serializable {
    /**
     * 终端物理地址
     */
    private String MACAddress;
    /**
     * 此物理地址第一次采集到的时间
     */
    private long firstCollectTime;
    /**
     * 此物理地址最后一次采集到的时间
     */
    private long lastCollectTime;
    /**
     * 扫描次数
     */
    private int scanTime;
    /**
     * 被采集设备终端场强
     */
    private double fieldIntensity;
    /**
     * 1 表示进入，0表示离开
     */
    private int inOutFlag;
    /**
     * 终端历史SSID，多个之间以分号分隔
     */
    private String historySSID;
    /**
     * 终端设备相对于采集设备的X坐标（正东方向），单位：米（m），未知填写0
     */
    private double x;
    /**
     * 终端设备相对于采集设备的Y坐标（正北方向），单位：米（m），未知填写0
     */
    private double y;
    /**
     * 连接的mac地址信息
     */
    private String APMacAddress;
    /**
     * 终端品牌
     */
    private String terminalBrand;

    private String siteNo;

    @Override
    public String getSiteNo() {
        return siteNo;
    }

    @Override
    public void setSiteNo(String siteNo) {
        this.siteNo = siteNo;
    }

    public String getMACAddress() {
        return MACAddress;
    }

    public void setMACAddress(String mACAddress) {
        MACAddress = mACAddress;
    }

    public long getFirstCollectTime() {
        return firstCollectTime;
    }

    public void setFirstCollectTime(long firstCollectTime) {
        this.firstCollectTime = firstCollectTime;
    }

    public long getLastCollectTime() {
        return lastCollectTime;
    }

    public void setLastCollectTime(long lastCollectTime) {
        this.lastCollectTime = lastCollectTime;
    }

    public int getScanTime() {
        return scanTime;
    }

    public void setScanTime(int scanTime) {
        this.scanTime = scanTime;
    }

    public double getFieldIntensity() {
        return fieldIntensity;
    }

    public void setFieldIntensity(double fieldIntensity) {
        this.fieldIntensity = fieldIntensity;
    }

    public int getInOutFlag() {
        return inOutFlag;
    }

    public void setInOutFlag(int inOutFlag) {
        this.inOutFlag = inOutFlag;
    }

    public String getHistorySSID() {
        return historySSID;
    }

    public void setHistorySSID(String historySSID) {
        this.historySSID = historySSID;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String getAPMacAddress() {
        return APMacAddress;
    }

    public void setAPMacAddress(String aPMacAddress) {
        APMacAddress = aPMacAddress;
    }

    public String getTerminalBrand() {
        return terminalBrand;
    }

    public void setTerminalBrand(String terminalBrand) {
        this.terminalBrand = terminalBrand;
    }

}

