package com.mgunter.f1streamcode;

import io.ppatierno.formula1.Driver;

public class RaceInfoEvent {
    private Integer carPosition = 0;
    private String driverId;

    public Float getMetersToLeader() {
        return metersToLeader;
    }

    public void setMetersToLeader(Float metersToLeader) {
        this.metersToLeader = metersToLeader;
    }

    public Float getMetersFromFollower() {
        return metersFromFollower;
    }

    public void setMetersFromFollower(Float metersFromFollower) {
        this.metersFromFollower = metersFromFollower;
    }

    public Float getSecsToLeader() {
        return secsToLeader;
    }

    public void setSecsToLeader(Float secsToLeader) {
        this.secsToLeader = secsToLeader;
    }

    public Float getSecsFromFollower() {
        return secsFromFollower;
    }

    public void setSecsFromFollower(Float secsFromFollower) {
        this.secsFromFollower = secsFromFollower;
    }

    public Integer getSpeedInKph() {
        return speedInKph;
    }

    public void setSpeedInKph(Integer speedInKph) {
        this.speedInKph = speedInKph;
    }

    private Float metersToLeader;
    private Float metersFromFollower;
    private Float secsToLeader;
    private Float secsFromFollower;
    private Integer speedInKph;

    public RaceInfoEvent(String driverId) {
        this.driverId = driverId;
    }

    public Driver getDriverDetails() {
        return driverDetails;
    }

    public void setDriverDetails(Driver driverDetails) {
        this.driverDetails = driverDetails;
    }

    private Driver driverDetails;

    public Integer getCarPosition() {
        return carPosition;
    }

    public void setCarPosition(Integer carPosition) {
        this.carPosition = carPosition;
    }

    public String getDriverId() {
        return driverId;
    }

    public void setDriverId(String driverId) {
        this.driverId = driverId;
    }

    public Float getLapPosition() {
        return lapPosition;
    }

    public void setLapPosition(Float lapPosition) {
        this.lapPosition = lapPosition;
    }

    public Integer getLap() {
        return lap;
    }

    public void setLap(Integer lap) {
        this.lap = lap;
    }

    private Float lapPosition;
    private Integer lap;


}

