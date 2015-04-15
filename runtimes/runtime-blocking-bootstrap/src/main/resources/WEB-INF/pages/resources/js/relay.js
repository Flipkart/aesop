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
        Relay.Dashboard.bindKeyUpEvent();
        Relay.Dashboard.bindDocumentClickEvent();
    },
    bindDocumentClickEvent: function(){
        $(document).on('click', function(event){

            var elem = $(event.target);
            if(elem.attr('id') == 'closeModal' || elem.attr('id') == 'overlay') {
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
    }
};

$(document).ready(function(){
    Relay.Dashboard.init();
});
