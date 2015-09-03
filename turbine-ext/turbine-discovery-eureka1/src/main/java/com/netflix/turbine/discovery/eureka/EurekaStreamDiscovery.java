/**
 * Copyright 2014 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.discovery.eureka;

import java.net.URI;
import java.util.Arrays;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamDiscovery;

import org.apache.commons.lang.StringUtils;
import rx.Observable;

public class EurekaStreamDiscovery implements StreamDiscovery {

    public static EurekaStreamDiscovery create(String appName, String uriTemplate) {
        return new EurekaStreamDiscovery(appName, uriTemplate);
    }

    public final static String HOSTNAME = "{HOSTNAME}";
    private final String uriTemplate;
    private final String appName;

    private EurekaStreamDiscovery(String appName, String uriTemplate) {
        this.appName = appName;
        this.uriTemplate = uriTemplate;
    }

    @Override
    public Observable<StreamAction> getInstanceList() {
        String[] appNames = appName.split(",");
        return Observable.from(Arrays.asList(appNames))
                .flatMap(appName -> {
                    return new EurekaInstanceDiscovery()
                            .getInstanceEvents(appName)
                            .map(ei -> {
                                return getStreamAction(ei);

                            });
                });
    }

    private StreamAction getStreamAction(EurekaInstance ei) {
        URI uri;
        try {
            String hostName = ei.getHostName();
            /*if (hasIP(ei)) {
                // set the IP to be the hostname (if IP is used then it's better than not having one)
                hostName = ei.getIPAddr();
            }
            if (hostName.startsWith("ip-")) {
                hostName = hostName.substring(3).replaceAll("-",".");
            }*/
            uri = new URI(uriTemplate.replace(HOSTNAME, hostName));
        } catch (Exception e) {
            throw new RuntimeException("Invalid URI", e);
        }
        if (ei.getStatus() == EurekaInstance.Status.UP) {
            return StreamAction.create(StreamAction.ActionType.ADD, uri);
        } else {
            return StreamAction.create(StreamAction.ActionType.REMOVE, uri);
        }
    }

    public boolean hasIP(EurekaInstance instanceInfo) {
        return StringUtils.isNotBlank(instanceInfo.getIPAddr());
    }

}
