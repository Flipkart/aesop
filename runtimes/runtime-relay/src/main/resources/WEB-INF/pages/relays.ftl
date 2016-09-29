<#include "header.ftl">

<div class="section">
    <h2>Registered Relays</h2>

    <div>
        <table cellspacing=0 cellpadding=0 class="relays">
            <tr>
                <th colspan="2">Physical Source</th>
                <th rowspan="2">Logical Source</th>
                <th colspan="3">Producer</th>
                <th rowspan="2">Consumers</th>
            </tr>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Consumers</th>
                <th>Last SCN</th>
                <th>Binlog</th>
            </tr>
        <#list relayInfos as relay>
            <tr>
                <td>${relay.getpSourceId()}</td>
                <td>${relay.getpSourceName()}</td>
                <#if (relay.lSourceInfos?size > 0)>
                    <td>
                    ${relay.lSourceInfos?size}
                    </td>
                <#else>
                    <td>
                        None
                    </td>
                </#if>
                <td class="v">${relay.clientInfos?size}</td>
                <td class="v">${relay.getProducerSinceSCN()}</td>
                <td>${relay.fetchProducerSinceSCNBinLong()?c}</td>
                <td>
                    <table cellspacing=0 cellpadding=0 class="relays">
                        <tr>
                            <th rowspan="2">
                                Host
                            </th>
                            <th rowspan="2">Partitions</th>
                            <th colspan="3">
                                Trailing SCN
                            </th>
                            <th colspan="3">
                                Leading SCN
                            </th>
                        </tr>
                        <tr>
                            <th>SCN</th>
                            <th>Binlog</th>
                            <th>Lag</th>
                            <th>SCN</th>
                            <th>Binlog</th>
                            <th>Lag</th>
                        </tr>

                        <#list relay.hostGroupedClient?keys as clientHost>
                            <tr>
                                <td>
                                    <a class="client" href="#" data-id="${relay.getpSourceId()}"
                                       data-clientHost="${ clientHost }">${ clientHost }</a>
                                </td>
                                <td>${relay.hostGroupedClient[clientHost]["PARTITIONS"]?c}</td>
                                <td>${relay.hostGroupedClient[clientHost]["MIN"]?c}</td>
                                <td>${relay.hostGroupedClient[clientHost]["MIN-BINLOG"]?c}</td>
                                <td>${(relay.fetchProducerSinceSCNInLong()-relay.hostGroupedClient[clientHost]["MIN"])?c}</td>
                                <td>${relay.hostGroupedClient[clientHost]["MAX"]?c}</td>
                                <td>${relay.hostGroupedClient[clientHost]["MAX-BINLOG"]?c}</td>
                                <td>${(relay.fetchProducerSinceSCNInLong()-relay.hostGroupedClient[clientHost]["MAX"])?c}</td>
                            </tr>
                        </#list>
                    </table>
                </td>
            </tr>
        </#list>
        </table>
    </div>

    <div class="clearfix"></div>

</div>

<div id="overlay"></div>
<div id="modal">
    <div id="modalContent"></div>
</div>

<script type="text/javascript">
    Relay.Client_SCN = ${ relayClientGrouped };
</script>
<#include "footer.ftl">
