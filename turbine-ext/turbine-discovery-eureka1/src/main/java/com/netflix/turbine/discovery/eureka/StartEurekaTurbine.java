/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.turbine.discovery.eureka;

import com.netflix.appinfo.providers.CloudInstanceConfigProvider;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.providers.DefaultEurekaClientConfigProvider;
import com.netflix.discovery.shared.Applications;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.turbine.Turbine;

import java.util.stream.Collector;
import java.util.stream.Collectors;

public class StartEurekaTurbine {
    private static final Logger logger = LoggerFactory.getLogger(StartEurekaTurbine.class);

    public static void main(String[] args) {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts("port").withRequiredArg();
        optionParser.accepts("app").withRequiredArg();
        optionParser.accepts("urlTemplate").withRequiredArg();

        OptionSet options = optionParser.parse(args);
        int port = -1;
        if (!options.has("port")) {
            System.err.println("Argument -port required for SSE HTTP server to start on. Eg. -port 8888");
            System.exit(-1);
        } else {
            try {
                port = Integer.parseInt(String.valueOf(options.valueOf("port")));
            } catch (NumberFormatException e) {
                System.err.println("Value of port must be an integer but was: " + options.valueOf("port"));
            }
        }

        String app = null;
        if (!options.has("app")) {

            logger.info("No app specified. Will retrieve and use list of apps from Eureka.");
            DiscoveryManager.getInstance().initComponent(new CloudInstanceConfigProvider().get(), new DefaultEurekaClientConfigProvider().get());
            Applications applications = DiscoveryManager.getInstance().getDiscoveryClient().getApplications();
            app = applications.getRegisteredApplications().stream().map(application -> application.getName()).collect(Collectors.joining(","));
            logger.info("Found the following apps: " + app);

        } else {
            app = String.valueOf(options.valueOf("app"));
        }

        String template = null;
        if (!options.has("urlTemplate")) {
            System.err.println("Argument -urlTemplate required. Eg. http://" + EurekaStreamDiscovery.HOSTNAME + "/metrics.stream");
            System.exit(-1);
        } else {
            template = String.valueOf(options.valueOf("urlTemplate"));
            if (!template.contains(EurekaStreamDiscovery.HOSTNAME)) {
                System.err.println("Argument -urlTemplate must contain " + EurekaStreamDiscovery.HOSTNAME + " marker. Eg. http://" + EurekaStreamDiscovery.HOSTNAME + "/metrics.stream");
                System.exit(-1);
            }
        }

        logger.info("Turbine => Eureka App: " + app);
        logger.info("Turbine => Eureka URL Template: " + template);

        try {
            Turbine.startServerSentEventServer(port, EurekaStreamDiscovery.create(app, template));
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}
