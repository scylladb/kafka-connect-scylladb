package io.connect.scylladb;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.AddressTranslator;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.*;

class ComposeAddressTranslator implements AddressTranslator {

    public Map<InetSocketAddress, InetSocketAddress> addressMap = new HashMap<>();

    @Override
    public void init(Cluster cluster) {
    }

    public void setMap(String addressMapString) {
        JSONObject jsonmap;

        if (addressMapString.charAt(0) == '[') {
            JSONArray jsonarray = new JSONArray(addressMapString);
            System.out.println(jsonarray.toString());
            Iterator jai = jsonarray.iterator();

            while (jai.hasNext()) {
                JSONObject element = (JSONObject) jai.next();
                Iterator subpart = element.keys();
                String internal = (String) subpart.next();
                String external = element.getString(internal);
                addAddresses(internal, external);
            }
        } else {
            jsonmap = new JSONObject(addressMapString);
            Iterator keys = jsonmap.keys();
            while (keys.hasNext()) {
                String internal = (String) keys.next();
                String external = (String) jsonmap.getString(internal);
                addAddresses(internal, external);
            }
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
        return addressMap.values();
    }

    @Override
    public InetSocketAddress translate(final InetSocketAddress inetSocketAddress) {
        return addressMap.getOrDefault(inetSocketAddress, inetSocketAddress);
    }

    @Override
    public void close() {
    }
}
