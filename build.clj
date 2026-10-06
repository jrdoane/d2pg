(ns build
  "Packaging. The version is 0.1.<commit count>, so every commit on main gets
  a distinct, increasing one."
  (:require [clojure.tools.build.api :as b]))

(def lib 'io.github.jrdoane/d2pg)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar
  "The library jar, sources only. The host application picks the Datomic
  client and logging backend."
  [_]
  (clean nil)
  (let [basis (b/create-basis {:project "deps.edn"})]
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :pom-data [[:licenses
                              [:license
                               [:name "Apache License, Version 2.0"]
                               [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]})
    (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
    (b/jar {:class-dir class-dir
            :jar-file (format "target/d2pg-%s.jar" version)})))

(defn uber
  "A standalone CLI: java -jar target/d2pg-<version>-standalone.jar config.edn.
  It bundles Datomic Local and slf4j-simple; build with other aliases on the
  basis (e.g. a client-pro or client-cloud dependency) for those deployments."
  [{:keys [aliases] :or {aliases [:run]}}]
  (clean nil)
  (let [basis (b/create-basis {:project "deps.edn" :aliases aliases})]
    (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
    (b/compile-clj {:basis basis :ns-compile '[d2pg] :class-dir class-dir})
    (b/uber {:class-dir class-dir
             :uber-file (format "target/d2pg-%s-standalone.jar" version)
             :basis basis
             :main 'd2pg})))
