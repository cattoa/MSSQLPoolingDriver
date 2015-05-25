/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.wilderness;

import java.util.EnumMap;
import java.util.Map;

/**
 *
 * @author Allistair
 */
public class ExternalServiceStatus {
    private Map<ExternalService,ProcessingStatus> serviceStatus = new EnumMap<>(ExternalService.class);

    public ExternalServiceStatus(){
        
        for(ExternalService extService : ExternalService.values()){
            this.serviceStatus.put(extService,ProcessingStatus.STOPPED);
        }
    
    }
    
    public ProcessingStatus getServiceStatus(ExternalService extService) {
        return this.serviceStatus.get(extService);
    }

    public boolean setServiceStatus(ExternalService extService ,ProcessingStatus status) {
        ProcessingStatus currentStatus = this.serviceStatus.get(extService);
        if (status == ProcessingStatus.STARTING){
            if (currentStatus == ProcessingStatus.STARTING){
                return false;
            }
        }
        
        this.serviceStatus.put(extService, status);
        System.out.println("Current Statusc: " + currentStatus.name());
        System.out.println("Set Status :" + status.name() + " : " + extService.name());
        return true;
    }

    
    
}
