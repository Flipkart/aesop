<#include "header.ftl">

    <div class="section">
	<h2>Registered Relays</h2>
        <div class="col-2">
            <table cellspacing=0 cellpadding=0 class="relays">
                <tr>
                    <th colspan="2">Physical Source</th>
                    <th rowspan="2">Logical Source</th>
                    <th colspan="2">Producer</th>
                </tr>
                <tr>
                    <th>ID</th>
                    <th>Name</th>
                    <th>Consumers</th>
                    <th>Last SCN</th>
                </tr>
            <#list relayInfos as relay>
                <tr>
                    <td >${relay.getpSourceId()}</td>
                    <td >${relay.getpSourceName()}</td>
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
                </tr>
            </#list>
            </table>
        </div>
        <#if relayInfos?size gt 0 >
        <div class="col-2">
            <table cellspacing=0 cellpadding=0 class="relays">
                <tr>
                    <th colspan="3">Consumers</th>
                </tr>
                <tr>
                    <th>
                    Host
                </th>
                    <th>
                    Trailing SCN
                </th>
                    <th>
                    Leading SCN
                </th>
                </tr>

                <#list relayInfos[0].hostGroupedClient?keys as clientHost>
                <tr>
                    <td>
                        <a class="client" href="#" data-id="${relayInfos[0].getpSourceId()}" data-clientHost="${ clientHost }">${ clientHost }</a>
                    </td>
                    <td>${ relayInfos[0].hostGroupedClient[clientHost]["MIN"]?c }</td>
                    <td>${ relayInfos[0].hostGroupedClient[clientHost]["MAX"]?c }</td>
                </tr>
                </#list>
            </table>
        </div>
        </#if>
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
