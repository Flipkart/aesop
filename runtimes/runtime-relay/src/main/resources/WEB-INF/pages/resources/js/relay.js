// Jquery Plugins to fetch query format from get params & hash string
(function($) {
    $.HashString = (function(a) {
        if (a == "") return {};
        var b = {};
        for (var i = 0; i < a.length; ++i)
        {
            var p=a[i].split('=');
            if (p.length != 2) continue;
            b[p[0]] = decodeURIComponent(p[1].replace(/\+/g, " "));
        }
        return b;
    })(window.location.hash.substr(1).split('&'));

    $.QueryString = (function(a) {
        if (a == "") return {};
        var b = {};
        for (var i = 0; i < a.length; ++i)
        {
            var p=a[i].split('=');
            if (p.length != 2) continue;
            b[p[0]] = decodeURIComponent(p[1].replace(/\+/g, " "));
        }
        return b;
    })(window.location.search.substr(1).split('&'))
})(jQuery);


Relay = {};

Relay.Views = {
    renderClientInfoTable : function(model) {
        var clientTable = $('<table />')
            .html(
            $('<tr />').html($('<th />').text('Partition')).append($('<th />').text('SCN'))
        ).addClass('relays');
        for(var partition in model) {
            var scn = model[partition];
            clientTable.append(
                $('<tr />').html(
                    $('<td />').text(partition)
                ).append(
                    $('<td />').text(scn)
                )
            );
        }

        return clientTable;
    },
    showModal: function(content) {
        $('#modal #modalContent').html(content);
        $('#overlay, #modal').show();
    },
    hideModal: function(){
        $('#overlay, #modal').hide();
        window.location.hash = '';
    }
};

Relay.Dashboard = {
    init : function() {
        Relay.Dashboard.handleWindowHash();
        Relay.Dashboard.bindKeyUpEvent();
        Relay.Dashboard.bindDocumentClickEvent();
    },
    bindDocumentClickEvent: function(){
        $(document).on('click', function(event){

            var elem = $(event.target);
            if(elem.hasClass('client')) {
                event.preventDefault();
                Relay.Dashboard.handleClientDetailClick(elem);
            } else if(elem.attr('id') == 'closeModal' || elem.attr('id') == 'overlay') {
                Relay.Views.hideModal();
            }

        });
    },
    bindKeyUpEvent : function() {
        $(document).on('keyup', function(){
            if(event.keyCode === 27) {
                Relay.Views.hideModal();
            }
        });
    },
    handleClientDetailClick: function(elem){
        var pId = elem.data('id');
        var clientHost = elem.data('clienthost');
        window.location.hash = "#id="+pId+"&clientHost="+clientHost;
        Relay.Dashboard.showClientInfoModal(pId, clientHost);
    },
    handleWindowHash: function() {
        if(window.location.hash.length > 1 && $.HashString['id']) {
            Relay.Dashboard.showClientInfoModal($.HashString['id'], $.HashString['clientHost']);
        }
    },
    showClientInfoModal : function(pId, clientHost) {
        var view = Relay.Views.renderClientInfoTable(Relay.Client_SCN[pId][clientHost]);
        Relay.Views.showModal(view);
    }
};

$(document).ready(function(){
    Relay.Dashboard.init();
});
