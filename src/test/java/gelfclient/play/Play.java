/*
 * Copyright 2014 TORCH GmbH
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

package gelfclient.play;

import org.graylog2.gelfclient.GelfConfiguration;
import org.graylog2.gelfclient.GelfMessage;
import org.graylog2.gelfclient.GelfMessageLevel;
import org.graylog2.gelfclient.GelfTransports;
import org.graylog2.gelfclient.transport.GelfTransport;

import java.util.concurrent.TimeUnit;

/**
 * @author Bernd Ahlers <bernd@torch.sh>
 */
public class Play {
    public static void main(String... args) throws InterruptedException {

        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");

        final GelfConfiguration config = new GelfConfiguration("localhost", 12201)
                .transport(GelfTransports.TCP)
                .enableTls()
                //.disableTls()
                .disableTlsCertVerification()
                .tcpKeepAlive(true)
                .queueSize(1024)
                .reconnectDelay(5000);

        final GelfTransport transport = GelfTransports.create(config);

        int count = 0;
        while (true) {
            final GelfMessage msg = new GelfMessage("Hello world!! " + count + " " + config.getTransport().toString());
            msg.setLevel(GelfMessageLevel.DEBUG);
            msg.setFullMessage("Full message");

            count++;

            msg.addAdditionalField("_count", count);
            msg.addAdditionalField("_oink", 1.231);
            msg.addAdditionalField("_facility", "gelf-netty-client");
            //msg.addAdditionalField("_objecttest", new Object());

            transport.send(msg);
            TimeUnit.SECONDS.sleep(2);
        }
    }
}
