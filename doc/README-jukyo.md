jageocoder 用住所データベース利用規約（住居表示レベル）
2024年1月6日 株式会社情報試作室

1. データ形式について

    このデータは jageocoder 用の住所データベースファイルです。

    ファイル名末尾が "_v1*.zip" (* は数字) のものは v1 系のフォーマットで、
    展開すると "address.db" という名前の SQLite3 ファイルが含まれています。

    ファイル名末尾が "_v2*.zip" (* は数字) のものは v2 系のフォーマットで、
    PortableTab フォーマットのバイナリファイルが含まれています。

2. データの出典について

    このデータは以下の情報を組み合わせて作成しています。

    (1) 都道府県・市区町村の代表点

        ROIS-DS人文学オープンデータ共同利用センターが公開している
        「歴史的行政区域データセットβ版地名辞書」から住所要素名
        および座標を抽出しました。

        出典 歴史的行政区域データセットβ版地名辞書
        https://geonlp.ex.nii.ac.jp/dictionary/geoshape-city/
        - 2022年度版 （2024-01-06 ダウンロード）
        - ライセンス CC-BY 4.0

    (2) 大字レベルおよび街区レベル

        国交省の「位置参照情報ダウンロード」サイトから取得したデータより、
        大字レベルと街区レベルの住所要素名および座標を抽出しました。

        出典 位置参照方法ダウンロードサイト
        https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi
        - 大字町丁目レベル位置参照情報（令和4年） 16.0b （2024-01-06 ダウンロード）
        - 街区レベル位置参照方法（令和4年） 21.0a （2024-01-06 ダウンロード）

    (3) Geolonia 住所データ

        株式会社 Geolonia がオープンデータとして公開している全国の町丁目
        レベルの住所データより、町丁目（大字）レベルの住所要素名および
        座標を抽出しました。

        出典 Geolonia 住所データ
        https://geolonia.github.io/japanese-addresses/
        - 2023-07 更新版 （2024-01-06 ダウンロード）
        - ライセンス CC-BY 4.0
        
    (4) 電子国土基本図（地名情報）「住居表示住所」
   
        国土地理院の『電子国土基本図（地名情報）「住居表示住所」』
        サイトから取得したデータより、住居表示住所の住所要素名および
        座標を抽出しました。

        出典 電子国土基本図（地名情報）「住居表示住所」
        https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html
        - 2024-01-01 更新版 （2024-01-06 ダウンロード）
        - 「測量法に基づく国土地理院長承認（使用）R 5JHs 31」による

    (5) 登記所備付地図
   
        法務局の「登記所備付地図」をＧ空間情報センターで GeoJSON に
        変換した変換済みデータを情報試作室がクレンジング、
        代表点座標を抽出しました。

        出典：「登記所備付地図データ」（法務省）
        を基にＧ空間情報センターにて変換処理した全市町村 GeoJSON
        
        - 令和4年度データ (2023-08-02 ダウンロード)
            https://front.geospatial.jp/moj-chizu-shp-download/

    (6) 日本郵便郵便番号データ

        日本郵便が公開している郵便番号データより、大字・字レベルの
        住所要素に対応する郵便番号（7桁コード）を付与しました。

        出典 郵便番号データ
        https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip
        - 2023-12-31 更新版 （2024-01-06 ダウンロード）
        - 郵便番号データは「自由に配布」が許可されています。
            https://www.post.japanpost.jp/zipcode/dl/readme.html

    (7) アドレス・ベース・レジストリ

        デジタル庁が公開している「日本 町字マスター データセット」より、
        字レベルの住所要素に対応する字ID（7桁コード）とを取得し、
        大字町丁目レベル位置参照情報および Geolonia 住所データの
        対応する要素に付与しました。

        また同データセットより字レベルのカナ表記・英字表記を取得し、
        都道府県、郡、市区町村、大字、字の表記として登録しました。

        上記 (2), (4) の欠損値を補完するため、「住居表示・街区マスター」、
        「住居表示・住所マスター」とそれぞれに対応する「位置参照拡張」を
        取得し、位置参照情報として利用しました。

        出典 アドレス・ベース・レジストリより
        - 「日本 町字マスター データセット」
        https://catalog.registries.digital.go.jp/rc/dataset/ba-o1-000000_g2-000003
        - 「全国 町字マスター位置参照拡張 データセット」
        https://catalog.registries.digital.go.jp/rc/dataset/ba000004
        - 「全国 住居表示・街区マスター データセット」
        https://catalog.registries.digital.go.jp/rc/dataset/ba000002
        - 「全国 住居表示・住所マスター データセット」
        https://catalog.registries.digital.go.jp/rc/dataset/ba000003
        - 「全国 住居表示－街区マスター位置参照拡張 データセット」
        https://catalog.registries.digital.go.jp/rc/dataset/ba000005
        - 「全国 住居表示－住所マスター位置参照拡張 データセット」
        https://catalog.registries.digital.go.jp/rc/dataset/ba000006
        - 2023-01-23 公開版 （2024-01-06 ダウンロード）
        - 利用規約に基づき加工したデータを配布しています
            https://www.digital.go.jp/policies/base_registry_address_tos/


3. データの利用条件

(1) 本データは、弊社が国土地理院より下記の承認を受けて作成し
    提供するものです。
   「測量法に基づく国土地理院長承認（使用）R 5JHs 31」

(2) 本データは、商用・非商用を問わず利用することができます。

(3) ［複製］
    任意の台数のサーバ・PC等に複製・配置・配布することができますが、
    必ずこの README.md をデータと同じ場所に置いてください。

(4) ［利用］
    本データを常時利用するアプリケーションやサービスを開発し
    公開する場合、利用者から見えるところ（ヘルプページ等）に
    以下の文言を記載してください。国土交通省の利用規約も満たします。

    「位置参照情報（大字町丁目・街区レベル）令和４年」（国土交通省）、
    「電子国土基本図（地名情報）住居表示住所」（国土地理院）、
    「Geolonia 住所データ」（株式会社Geolonia） https://geolonia.github.io/japanese-addresses/、
    「登記所備付データ」（法務局） https://front.geospatial.jp/moj-chizu-shp-download/、
    「アドレス・ベース・レジストリ」（デジタル庁）
    https://www.digital.go.jp/policies/base_registry_address_tos/
    をもとに、株式会社情報試作室が加工した
    jageocoder 用住所データベース（住居表示レベル）を利用

(5) ［二次利用］
    本データを利用して作成した図面や表、グラフ等を書籍や
    ウェブ等で公開する場合、出典や参考文献として以下の情報を
    記載してください。

    国土交通省「位置参照情報」を加工, https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi
    国土地理院「電子国土基本図住居表示住所」を加工, https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html
    Geolonia Inc. 「Geolonia 住所データ」を加工, https://geolonia.github.io/japanese-addresses/
    法務局「登記所備付データ」を加工, https://front.geospatial.jp/moj-chizu-shp-download/
    「アドレス・ベース・レジストリ」を加工,
    https://www.digital.go.jp/policies/base_registry_address_tos/

(6) ［免責］
    本データを利用した結果生じたいかなる損害についても
    弊社は責任を負いません。
    また、本データは予告なく変更または削除することがあります。

(7) ［問い合わせ先］
    株式会社情報試作室 お問い合わせ窓口
    info@info-proto.com
