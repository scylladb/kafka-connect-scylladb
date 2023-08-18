// Copyright (c) 2016 Compose, an IBM company
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package io.connect.scylladb;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterAddressTranslator implements AddressTranslator {

    public Map<InetSocketAddress, InetSocketAddress> addressMap = new HashMap<>();
    private static final Logger log = LoggerFactory.getLogger(ClusterAddressTranslator.class);

    public void setMap(String addressMapString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(addressMapString);
        Iterator<Map.Entry<String, JsonNode>> entries = jsonNode.fields();
        while(entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();
            addAddresses(entry.getKey(), entry.getValue().asText());
        }
    }

    public void addAddresses(String internal, String external) {
        String[] internalhostport = internal.split(":");
        String[] externalhostport = external.split(":");
        InetSocketAddress internaladdress = new InetSocketAddress(internalhostport[0], Integer.parseInt(internalhostport[1]));
        InetSocketAddress externaladdress = new InetSocketAddress(externalhostport[0], Integer.parseInt(externalhostport[1]));
        addressMap.put(internaladdress, externaladdress);
    }

    public Collection<InetSocketAddress> getContactPoints() {
        return Collections.unmodifiableCollection(addressMap.values());
    }

    @Override
    public InetSocketAddress translate(final InetSocketAddress inetSocketAddress) {
        return addressMap.getOrDefault(inetSocketAddress, inetSocketAddress);
    }

    @Override
    public void close() {
    }
}
