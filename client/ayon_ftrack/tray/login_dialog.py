import sys
import json

import requests
from qtpy import QtWidgets, QtCore, QtGui

from ayon_core import style
from ayon_core.resources import get_ayon_icon_filepath
from ayon_core.tools.utils import get_qt_app

from ayon_ftrack.lib import credentials
from ayon_ftrack.tray.login_tools import LoginServerThread


class _CredentialsDialog(QtWidgets.QDialog):
    SIZE_W = 300
    SIZE_H = 230

    login_changed = QtCore.Signal()
    logout_signal = QtCore.Signal()

    def __init__(self, parent=None):
        super().__init__(parent)

        self.setWindowTitle("AYON - Ftrack Login")
        self.setWindowIcon(QtGui.QIcon(get_ayon_icon_filepath()))
        self.setWindowFlags(
            QtCore.Qt.WindowCloseButtonHint
            | QtCore.Qt.WindowMinimizeButtonHint
        )

        self.setMinimumSize(QtCore.QSize(self.SIZE_W, self.SIZE_H))
        self.setMaximumSize(QtCore.QSize(self.SIZE_W + 100, self.SIZE_H + 100))
        self.setStyleSheet(style.load_stylesheet())

        # Inputs - user filling values
        inputs_widget = QtWidgets.QWidget(self)
        url_label = QtWidgets.QLabel("Ftrack URL:", inputs_widget)
        user_label = QtWidgets.QLabel("Username:", inputs_widget)
        api_label = QtWidgets.QLabel("API Key:", inputs_widget)

        url_input = QtWidgets.QLabel(inputs_widget)
        url_input.setTextInteractionFlags(
            QtCore.Qt.TextBrowserInteraction
        )
        url_input.setCursor(QtGui.QCursor(QtCore.Qt.IBeamCursor))

        user_input = QtWidgets.QLineEdit(inputs_widget)
        user_input.setPlaceholderText("user.name")

        api_input = QtWidgets.QLineEdit(inputs_widget)
        api_input.setPlaceholderText(
            "e.g. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        )

        input_layout = QtWidgets.QFormLayout(inputs_widget)
        input_layout.setContentsMargins(10, 15, 10, 5)
        input_layout.addRow(url_label, url_input)
        input_layout.addRow(user_label, user_input)
        input_layout.addRow(api_label, api_input)

        # Notes and errors for user
        labels_widget = QtWidgets.QWidget(self)
        note_label = QtWidgets.QLabel(
            (
                "NOTE: Click on \"Login\" button to log with your default"
                " browser or click on \"Advanced\" button to enter"
                " API key manually."
            ),
            labels_widget
        )
        note_label.setWordWrap(True)
        note_label.setVisible(False)

        error_label = QtWidgets.QLabel("", labels_widget)
        error_label.setWordWrap(True)
        error_label.setVisible(False)

        label_layout = QtWidgets.QVBoxLayout(labels_widget)
        label_layout.setContentsMargins(10, 5, 10, 5)
        label_layout.addWidget(note_label)
        label_layout.addWidget(error_label)

        bts_widget = QtWidgets.QWidget(self)
        btn_advanced = QtWidgets.QPushButton("Advanced", bts_widget)

        btn_simple = QtWidgets.QPushButton("Simple", bts_widget)

        btn_login = QtWidgets.QPushButton("Login", bts_widget)
        btn_login.setToolTip(
            "Set Username and API Key with entered values"
        )

        btn_ftrack_login = QtWidgets.QPushButton("Ftrack login", bts_widget)
        btn_ftrack_login.setToolTip("Open browser for Login to Ftrack")

        btn_logout = QtWidgets.QPushButton("Logout", bts_widget)

        btn_close = QtWidgets.QPushButton("Close", bts_widget)
        btn_close.setToolTip("Close this window")

        btns_layout = QtWidgets.QHBoxLayout(bts_widget)
        btns_layout.setContentsMargins(0, 0, 0, 0)
        btns_layout.addWidget(btn_advanced, 0)
        btns_layout.addWidget(btn_simple, 0)
        btns_layout.addStretch(1)
        btns_layout.addWidget(btn_ftrack_login, 0)
        btns_layout.addWidget(btn_login, 0)
        btns_layout.addWidget(btn_logout, 0)
        btns_layout.addWidget(btn_close, 0)

        main_layout = QtWidgets.QVBoxLayout(self)
        main_layout.addWidget(inputs_widget, 0)
        main_layout.addWidget(labels_widget, 0)
        main_layout.addStretch(1)
        main_layout.addWidget(bts_widget, 0)

        show_timer = QtCore.QTimer()
        show_timer.setInterval(0)

        ftrack_login_timer = QtCore.QTimer()
        ftrack_login_timer.setInterval(50)

        show_timer.timeout.connect(self._on_show_timer)
        ftrack_login_timer.timeout.connect(self._on_ftrack_login_timer)
        user_input.textChanged.connect(self._user_changed)
        api_input.textChanged.connect(self._api_changed)
        btn_advanced.clicked.connect(self._on_advanced_clicked)
        btn_simple.clicked.connect(self._on_simple_clicked)
        btn_login.clicked.connect(self._on_login_clicked)
        btn_ftrack_login.clicked.connect(self._on_ftrack_login_clicked)
        btn_logout.clicked.connect(self._on_logout_clicked)
        btn_close.clicked.connect(self._close_widget)

        self._url_label = url_label
        self._url_input = url_input
        self._user_label = user_label
        self._user_input = user_input
        self._api_label = api_label
        self._api_input = api_input

        self._error_label = error_label
        self._note_label = note_label

        self._btn_advanced = btn_advanced
        self._btn_simple = btn_simple
        self._btn_login = btn_login
        self._btn_ftrack_login = btn_ftrack_login
        self._btn_logout = btn_logout
        self._btn_close = btn_close

        self._show_timer = show_timer
        self._show_counter = 0

        self._ftrack_login_timer = ftrack_login_timer
        self._waiting_for_ftrack_login = False
        self._ftrack_login_result = None

        self._login_server_thread = None
        self._is_logged = None
        self._in_advance_mode = None
        self._set_advanced_mode(False)
        self._set_is_logged(False)

    def showEvent(self, event):
        super().showEvent(event)
        self._show_timer.start()

    def _on_show_timer(self):
        if self._show_counter < 2:
            self._show_counter += 1
            return
        self._show_counter = 0
        self._show_timer.stop()
        self._fill_ftrack_url()

    def closeEvent(self, event):
        self._cleanup()
        super().closeEvent(event)

    def set_credentials(self, username, api_key, is_logged=True):
        self._user_input.setText(username)
        self._api_input.setText(api_key)

        self._error_label.setVisible(False)

        for widget in (
            self._url_input,
            self._user_input,
            self._api_input,
        ):
            self._set_widget_state(widget, True)

        if is_logged is not None:
            self._set_is_logged(is_logged)

    def get_credentials(self):
        if self._is_logged:
            return self._user_input.text(), self._api_input.text()
        return None, None

    def _fill_ftrack_url(self):
        checked_url = self._check_url()
        if checked_url == self._url_input.text():
            return

        self._url_input.setText(checked_url or "< Not set >")

        enabled = bool(checked_url)

        for widget in (
            self._btn_login,
            self._btn_ftrack_login,
            self._api_input,
            self._user_input,
        ):
            widget.setEnabled(enabled)

        if not checked_url:
            for widget in (
                self._btn_advanced,
                self._btn_simple,
                self._btn_ftrack_login,
                self._btn_login,
                self._note_label,
                self._api_input,
                self._user_input,
            ):
                widget.setVisible(False)

    def _update_advanced_logged_visibility(self):
        is_advanced = self._in_advance_mode
        is_logged = self._is_logged

        advanced_visible = not is_logged and is_advanced
        for widget in (
            self._btn_login,
            self._btn_simple,
        ):
            widget.setVisible(advanced_visible)

        login_visible = not is_logged and not is_advanced
        for widget in (
            self._note_label,
            self._btn_ftrack_login,
            self._btn_advanced,
        ):
            widget.setVisible(login_visible)

        user_api_visible = is_logged or is_advanced
        for widget in (
            self._user_label,
            self._user_input,
            self._api_label,
            self._api_input,
        ):
            widget.setVisible(user_api_visible)

    def _set_advanced_mode(self, is_advanced):
        if self._in_advance_mode == is_advanced:
            return

        self._in_advance_mode = is_advanced

        self._error_label.setVisible(False)

        self._update_advanced_logged_visibility()

        if is_advanced:
            self._user_input.setFocus()
        else:
            self._btn_ftrack_login.setFocus()

    def _set_is_logged(self, is_logged):
        if self._is_logged == is_logged:
            return

        self._is_logged = is_logged

        for input_widget in (
            self._user_input,
            self._api_input,
        ):
            input_widget.setReadOnly(is_logged)
            input_widget.setCursor(QtGui.QCursor(QtCore.Qt.IBeamCursor))

        self._btn_logout.setVisible(is_logged)

        self._update_advanced_logged_visibility()

    def _set_error(self, msg):
        self._error_label.setText(msg)
        self._error_label.setVisible(True)

    def _on_logout_clicked(self):
        self._user_input.setText("")
        self._api_input.setText("")
        self._set_is_logged(False)
        self.logout_signal.emit()

    def _on_simple_clicked(self):
        self._set_advanced_mode(False)

    def _on_advanced_clicked(self):
        self._set_advanced_mode(True)

    def _user_changed(self):
        self._set_widget_state(self._user_input, True)

    def _api_changed(self):
        self._set_widget_state(self._api_input, True)

    def _set_widget_state(self, input_widget, valid):
        stylesheet = "" if valid else "border: 1px solid red;"
        input_widget.setStyleSheet(stylesheet)

    def _close_widget(self):
        self.close()

    def _on_login(self):
        self.login_changed.emit()
        self._set_is_logged(True)
        self._close_widget()

    def _on_login_clicked(self):
        username = self._user_input.text().strip()
        api_key = self._api_input.text().strip()
        missing = []
        if username == "":
            missing.append("Username")
            self._set_widget_state(self._user_input, False)

        if api_key == "":
            missing.append("API Key")
            self._set_widget_state(self._api_input, False)

        if len(missing) > 0:
            self._set_error("You didn't enter {}".format(" and ".join(missing)))
            return

        if not self._login_with_credentials(username, api_key):
            self._set_widget_state(self._user_input, False)
            self._set_widget_state(self._api_input, False)
            self._set_error(
                "We're unable to sign in to Ftrack with these credentials"
            )

    def _login_with_credentials(self, username, api_key):
        verification = credentials.check_credentials(username, api_key)
        if verification:
            credentials.save_credentials(username, api_key, False)
            self.set_credentials(username, api_key)
            self._on_login()
        return verification

    def _cleanup_login_server_thread(self):
        if self._login_server_thread is None:
            return
        thread, self._login_server_thread = self._login_server_thread, None
        if thread.is_alive():
            thread.stop()
        thread.join()

    def _on_ftrack_login_clicked(self):
        url = self._check_url()
        if not url:
            return

        # If there is an existing server thread running we need to stop it.
        self._cleanup_login_server_thread()

        # If credentials are not properly set, try to get them using a http
        # server.
        self._waiting_for_ftrack_login = True
        self._ftrack_login_timer.start()

        self._login_server_thread = LoginServerThread(
            url, self._result_of_ftrack_thread
        )
        self._login_server_thread.start()

    def _result_of_ftrack_thread(self, username, api_key):
        self._ftrack_login_result = (username, api_key)
        self._waiting_for_ftrack_login = False

    def _on_ftrack_login_timer(self):
        if self._waiting_for_ftrack_login:
            return

        self._ftrack_login_timer.stop()
        self._cleanup_login_server_thread()

        username, api_key = self._ftrack_login_result
        if not self._login_with_credentials(username, api_key):
            self._set_widget_state(self._api_input, False)
            self._set_error((
                "Somthing happened with Ftrack login."
                " Try enter Username and API key manually."
            ))

    def _cleanup(self):
        self._cleanup_login_server_thread()
        self._ftrack_login_timer.stop()
        self._waiting_for_ftrack_login = False

    def _get_source_ftrack_url(self):
        # NOTE This must be overriden
        return None

    def _check_url(self):
        url = self._get_source_ftrack_url()
        if url is None:
            self._set_error(
                "Specified URL does not lead to a valid Ftrack server."
            )
            return

        try:
            result = requests.get(
                url,
                # Old python API will not work with redirect.
                allow_redirects=False
            )
        except requests.exceptions.RequestException:
            self._set_error(
                "Specified URL could not be reached."
            )
            return

        if (
            result.status_code != 200
            or "FTRACK_VERSION" not in result.headers
        ):
            self._set_error(
                "Specified URL does not lead to a valid Ftrack server."
            )
            return
        return url


class PopupCredentialsDialog(_CredentialsDialog):
    def __init__(self, ftrack_url, parent=None):
        super().__init__(parent)

        self._ftrack_url = ftrack_url

    def _get_source_ftrack_url(self):
        return self._ftrack_url

    def _close_widget(self):
        username, api_key = self.get_credentials()
        if not username or not api_key:
            self.reject()
        else:
            self.accept()


class TrayCredentialsDialog(_CredentialsDialog):
    def __init__(self, addon, parent=None):
        super().__init__(parent)
        self._addon = addon

    def _on_login(self):
        username, api_key = self.get_credentials()
        self._addon.set_credentials_to_env(username, api_key)
        super()._on_login()

    def _get_source_ftrack_url(self):
        return self._addon.ftrack_url

    def _check_url(self):
        settings_url = self._addon.settings_ftrack_url
        if not settings_url:
            self._set_error(
                "Ftrack URL is not defined in settings!"
            )
            return

        return super()._check_url()

    def closeEvent(self, event):
        event.ignore()
        self._close_widget()

    def _close_widget(self):
        self._cleanup()
        self.hide()


def main():
    json_filepath = sys.argv[-1]
    with open(json_filepath, "r") as stream:
        data = json.load(stream)
    app = get_qt_app()  # noqa F841
    dialog = PopupCredentialsDialog(data["server_url"])
    dialog.exec_()
    username, api_key = dialog.get_credentials()
    data["username"] = username
    data["api_key"] = api_key
    with open(json_filepath, "w") as stream:
        json.dump(data, stream)


if __name__ == "__main__":
    main()
