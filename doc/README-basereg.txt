jageocoder 用住所データベース利用規約（住居表示レベル）
2022年12月31日 株式会社情報試作室

1. データ形式について

このデータは SQLite3 のデータベースファイル形式です。


2. データの出典について

このデータは以下の情報を組み合わせて作成しています。

(1) 都道府県・市区町村の代表点
    ROIS-DS人文学オープンデータ共同利用センターが公開している
    「歴史的行政区域データセットβ版地名辞書」から住所要素名
    および座標を抽出しました。

    出典 歴史的行政区域データセットβ版地名辞書
      https://geonlp.ex.nii.ac.jp/dictionary/geoshape-city/
      - 最終更新日時 2022-03-26T10:32:38+09:00
      - ライセンス CC-BY 4.0

(2) 大字レベルおよび街区レベル
    国交省の「位置参照情報ダウンロード」サイトから取得したデータより、
    大字レベルと街区レベルの住所要素名および座標を抽出しました。

    出典 位置参照方法ダウンロードサイト
      https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi
      - 大字町丁目レベル位置参照情報（令和３年） 15.0b
      - 街区レベル位置参照方法（令和３年） 20.0a

(3) Geolonia 住所データ
    株式会社 Geolonia がオープンデータとして公開している全国の町丁目
    レベルの住所データより、町丁目（大字）レベルの住所要素名および
    座標を抽出しました。

    出典 Geolonia 住所データ
      https://geolonia.github.io/japanese-addresses/
      - 2022-12-01 更新版 （2022-12-31 ダウンロード）
      - ライセンス CC-BY 4.0
      
(4) 国土地理院の『電子国土基本図（地名情報）「住居表示住所」』
    サイトから取得したデータより、住居表示住所の住所要素名および
    座標を抽出しました。

    出典 電子国土基本図（地名情報）「住居表示住所」
      https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html
      - 2022-11-16 更新版 （2023-01-06 ダウンロード）
      - 測量法による使用承認による

(5) 日本郵便郵便番号データ
    日本郵便が公開している郵便番号データより、大字・字レベルの
    住所要素に対応する郵便番号（7桁コード）を付与しました。

    出典 郵便番号データ
      https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip
      - 2022-12-28 更新版
      - 郵便番号データは「自由に配布」が許可されています。
        https://www.post.japanpost.jp/zipcode/dl/readme.html

(6) アドレス・ベース・レジストリ
    デジタル庁が公開している「日本 町字マスター データセット」より、
    字レベルの住所要素に対応する字ID（7桁コード）とを取得し、
    大字町丁目レベル位置参照情報および Geolonia 住所データの
    対応する要素に付与しました。

    また同データセットより字レベルのカナ表記・英字表記を取得し、
    都道府県、郡、市区町村、大字、字の表記として登録しました。

    上記 (2), (4) の欠損値を補完するため、「住居表示・街区マスター」、
    「住居表示・住所マスター」とそれぞれに対応する「位置参照拡張」を
    取得し、位置参照情報として利用しました。

    出典 「日本 町字マスター データセット」
      https://registry-catalog.registries.digital.go.jp/dataset/o1-000000_g2-000003
      都道府県単位の「町字マスター位置参照拡張 データセット」
      https://registry-catalog.registries.digital.go.jp/dataset/o1-xx0001_g2-000006
      都道府県単位の「住居表示・街区マスター データセット」
      https://registry-catalog.registries.digital.go.jp/dataset/o1-xx0001_g2-000004
      都道府県単位の「住居表示・住所マスター データセット」
      https://registry-catalog.registries.digital.go.jp/dataset/o1-xx0001_g2-000005
      都道府県単位の「住居表示－街区マスター位置参照拡張 データセット」
      https://registry-catalog.registries.digital.go.jp/dataset/o1-xx0001_g2-000007
      都道府県単位の「住居表示－住所マスター位置参照拡張 データセット」
      https://registry-catalog.registries.digital.go.jp/dataset/o1-xx0001_g2-000008
      ("xx"は"01"から"47"までの都道府県コード)
      - 2022-04-22 公開版 （2022-04-25 ダウンロード）
      - 利用規約に基づき加工したデータを配布しています
        https://www.digital.go.jp/policies/base_registry_address_tos/


3. データの利用条件

(1) 本データは、弊社が国土地理院より下記の承認を受けて作成し
    提供するものです。
   「測量法に基づく国土地理院長承認（使用）R 2JHs 1122」

(2) 本データは、商用・非商用を問わず利用することができます。

(3) ［複製］
    任意の台数のサーバ・PC等に複製・配置・配布することができますが、
    必ずこの README.txt をデータと同じ場所に置いてください。

(4) ［利用］
    本データを常時利用するアプリケーションやサービスを開発し
    公開する場合、利用者から見えるところ（ヘルプページ等）に
    以下の文言を記載してください。国土交通省の利用規約も満たします。

    「位置参照情報（大字町丁目・街区レベル）令和３年」（国土交通省）、
    「電子国土基本図（地名情報）住居表示住所 2022-11-16」（国土地理院）、
    「Geolonia 住所データ」（株式会社Geolonia） https://geolonia.github.io/japanese-addresses/、
    「アドレス・ベース・レジストリ」（デジタル庁）
    https://registry-catalog.registries.digital.go.jp/
    をもとに、株式会社情報試作室が加工した
    jageocoder 用住所データベース（住居表示レベル）を利用

(5) ［二次利用］
    本データを利用して作成した図面や表、グラフ等を書籍や
    ウェブ等で公開する場合、出典や参考文献として以下の情報を
    記載してください。

    国土交通省「位置参照情報」を加工, https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi, 2022-06-16.
    国土地理院「電子国土基本図住居表示住所」を加工, https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html, 2022-11-16.
    Geolonia Inc. 「Geolonia 住所データ」を加工, https://geolonia.github.io/japanese-addresses/, 2022-12-01.
    「アドレス・ベース・レジストリ 町字マスター」を加工,
    https://registry-catalog.registries.digital.go.jp/, 2022-04-25.

(6) ［免責］
    本データを利用した結果生じたいかなる損害についても
    弊社は責任を負いません。
    また、本データは予告なく変更または削除することがあります。

(7) ［問い合わせ先］
    株式会社情報試作室 お問い合わせ窓口
    info@info-proto.com
